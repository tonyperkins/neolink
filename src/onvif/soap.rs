//! Minimal SOAP, XML and HTTP helpers for the ONVIF PTZ service
//!
//! Only what ONVIF clients such as Frigate need: requests are parsed into a
//! small namespace-agnostic tree (elements are matched by local name) and
//! responses are written as strings.
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use quick_xml::{escape::escape, events::Event, Reader};
use sha1::{Digest, Sha1};
use std::time::Duration;

/// Largest request header block accepted
pub(super) const MAX_HEAD: usize = 16 * 1024;
/// Largest request body accepted (ONVIF requests are a few KiB)
pub(super) const MAX_BODY: usize = 256 * 1024;
/// Deepest element nesting accepted
const MAX_DEPTH: usize = 64;

pub(super) const SPACE_PT_VELOCITY: &str =
    "http://www.onvif.org/ver10/tptz/PanTiltSpaces/VelocityGenericSpace";
pub(super) const SPACE_PT_SPEED: &str =
    "http://www.onvif.org/ver10/tptz/PanTiltSpaces/GenericSpeedSpace";

/// An XML element, identified by its local name (namespace prefix dropped)
#[derive(Debug, Default, Clone)]
pub(super) struct Node {
    pub(super) name: String,
    pub(super) attrs: Vec<(String, String)>,
    pub(super) text: String,
    pub(super) children: Vec<Node>,
}

impl Node {
    pub(super) fn child(&self, name: &str) -> Option<&Node> {
        self.children.iter().find(|c| c.name == name)
    }

    pub(super) fn path(&self, path: &[&str]) -> Option<&Node> {
        path.iter().try_fold(self, |node, name| node.child(name))
    }

    pub(super) fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub(super) fn text(&self) -> &str {
        self.text.trim()
    }
}

fn local_name(qname: &[u8]) -> String {
    let name = String::from_utf8_lossy(qname);
    match name.rfind(':') {
        Some(i) => name[i + 1..].to_string(),
        None => name.into_owned(),
    }
}

fn start_node(e: &quick_xml::events::BytesStart) -> Result<Node> {
    let mut node = Node {
        name: local_name(e.name().as_ref()),
        ..Default::default()
    };
    for attr in e.attributes() {
        let attr = attr?;
        let key = attr.key.as_ref();
        if key == b"xmlns" || key.starts_with(b"xmlns:") {
            continue;
        }
        node.attrs
            .push((local_name(key), attr.unescape_value()?.into_owned()));
    }
    Ok(node)
}

/// Parse an XML document into its root element
pub(super) fn parse_xml(xml: &str) -> Result<Node> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    // stack[0] collects the root element
    let mut stack = vec![Node::default()];
    loop {
        match reader.read_event()? {
            Event::Start(e) => {
                if stack.len() > MAX_DEPTH {
                    bail!("XML nested too deeply");
                }
                stack.push(start_node(&e)?);
            }
            Event::Empty(e) => {
                let node = start_node(&e)?;
                stack
                    .last_mut()
                    .expect("stack holds the root collector")
                    .children
                    .push(node);
            }
            Event::End(_) => {
                if stack.len() < 2 {
                    bail!("Unbalanced XML");
                }
                let node = stack.pop().expect("checked length");
                stack
                    .last_mut()
                    .expect("checked length")
                    .children
                    .push(node);
            }
            Event::Text(t) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(&t.unescape()?);
                }
            }
            Event::CData(c) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(&String::from_utf8_lossy(&c));
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if stack.len() != 1 {
        bail!("Unclosed XML element");
    }
    stack
        .pop()
        .and_then(|root| root.children.into_iter().next())
        .ok_or_else(|| anyhow!("Empty XML document"))
}

/// The parts of a SOAP request the service uses
#[derive(Debug)]
pub(super) struct SoapRequest {
    pub(super) header: Option<Node>,
    /// The first element of the body, e.g. `ContinuousMove`
    pub(super) operation: Node,
}

pub(super) fn parse_soap(xml: &str) -> Result<SoapRequest> {
    let root = parse_xml(xml)?;
    if root.name != "Envelope" {
        bail!("Not a SOAP envelope");
    }
    let operation = root
        .child("Body")
        .and_then(|body| body.children.first())
        .cloned()
        .ok_or_else(|| anyhow!("Empty SOAP body"))?;
    Ok(SoapRequest {
        header: root.child("Header").cloned(),
        operation,
    })
}

/// A WS-Security UsernameToken from the SOAP header
#[derive(Debug)]
pub(super) struct UsernameToken {
    pub(super) username: String,
    password: String,
    digest: bool,
    nonce: String,
    created: String,
}

impl UsernameToken {
    pub(super) fn from_header(header: &Node) -> Option<Self> {
        let token = header.path(&["Security", "UsernameToken"])?;
        let password = token.child("Password")?;
        Some(Self {
            username: token.child("Username")?.text().to_string(),
            password: password.text().to_string(),
            digest: password
                .attr("Type")
                .is_some_and(|t| t.ends_with("#PasswordDigest")),
            nonce: token
                .child("Nonce")
                .map(|n| n.text().to_string())
                .unwrap_or_default(),
            created: token
                .child("Created")
                .map(|n| n.text().to_string())
                .unwrap_or_default(),
        })
    }

    /// Check the token against the user's password.
    ///
    /// Digest: Base64(SHA1(nonce + created + password)), with the nonce
    /// base64 decoded first. Text: the password itself.
    pub(super) fn verify(&self, password: &str) -> bool {
        if self.digest {
            let Ok(nonce) = BASE64.decode(self.nonce.as_bytes()) else {
                return false;
            };
            let mut hasher = Sha1::new();
            hasher.update(&nonce);
            hasher.update(self.created.as_bytes());
            hasher.update(password.as_bytes());
            let expected = BASE64.encode(hasher.finalize());
            constant_time_eq(expected.as_bytes(), self.password.as_bytes())
        } else {
            constant_time_eq(password.as_bytes(), self.password.as_bytes())
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Parse a simple xs:duration such as `PT10S`, `PT1.5S` or `PT1M30S`
pub(super) fn parse_duration(text: &str) -> Option<Duration> {
    let rest = text.trim().strip_prefix("PT")?;
    let mut secs = 0f64;
    let mut number = String::new();
    for c in rest.chars() {
        match c {
            '0'..='9' | '.' => number.push(c),
            'H' | 'M' | 'S' => {
                let value: f64 = number.parse().ok()?;
                number.clear();
                secs += value
                    * match c {
                        'H' => 3600.0,
                        'M' => 60.0,
                        _ => 1.0,
                    };
            }
            _ => return None,
        }
    }
    if !number.is_empty() || !secs.is_finite() || secs < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(secs))
}

/// Format a time as xs:dateTime in UTC
pub(super) fn xs_datetime(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

pub(super) fn esc(text: &str) -> std::borrow::Cow<'_, str> {
    escape(text)
}

const NAMESPACES: &str = concat!(
    r#"xmlns:s="http://www.w3.org/2003/05/soap-envelope" "#,
    r#"xmlns:ter="http://www.onvif.org/ver10/error" "#,
    r#"xmlns:tt="http://www.onvif.org/ver10/schema" "#,
    r#"xmlns:tds="http://www.onvif.org/ver10/device/wsdl" "#,
    r#"xmlns:trt="http://www.onvif.org/ver10/media/wsdl" "#,
    r#"xmlns:tptz="http://www.onvif.org/ver20/ptz/wsdl""#,
);

pub(super) fn envelope(body: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><s:Envelope {NAMESPACES}><s:Body>{body}</s:Body></s:Envelope>"#
    )
}

/// A SOAP 1.2 fault. `code` is `s:Sender` or `s:Receiver`, `subcode` an ONVIF
/// error such as `ter:NotAuthorized`
pub(super) fn fault(code: &str, subcode: &str, reason: &str) -> String {
    envelope(&format!(
        r#"<s:Fault><s:Code><s:Value>{code}</s:Value><s:Subcode><s:Value>{subcode}</s:Value></s:Subcode></s:Code><s:Reason><s:Text xml:lang="en">{}</s:Text></s:Reason></s:Fault>"#,
        esc(reason)
    ))
}

/// An HTTP request's head
#[derive(Debug, PartialEq, Eq)]
pub(super) struct HttpHead {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) host: Option<String>,
    pub(super) content_length: Option<usize>,
    pub(super) chunked: bool,
}

/// Parse the request line and headers (everything before the blank line)
pub(super) fn parse_http_head(head: &str) -> Result<HttpHead> {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or_else(|| anyhow!("Empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("No method"))?
        .to_string();
    let path = parts.next().ok_or_else(|| anyhow!("No path"))?.to_string();
    let mut parsed = HttpHead {
        method,
        path,
        host: None,
        content_length: None,
        chunked: false,
    };
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "host" => parsed.host = Some(value.to_string()),
            "content-length" => parsed.content_length = Some(value.parse()?),
            "transfer-encoding" => parsed.chunked = value.to_ascii_lowercase().contains("chunked"),
            _ => {}
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOVE: &str = r#"<?xml version='1.0' encoding='utf-8'?>
<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope">
  <soap-env:Header>
    <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <wsse:UsernameToken>
        <wsse:Username>frigate</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">vswM0WR7AJAjFucTHkHFQT/hgtg=</wsse:Password>
        <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">MDEyMzQ1Njc4OWFiY2RlZg==</wsse:Nonce>
        <wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">2026-11-10T22:00:00Z</wsu:Created>
      </wsse:UsernameToken>
    </wsse:Security>
  </soap-env:Header>
  <soap-env:Body>
    <ns0:ContinuousMove xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl">
      <ns0:ProfileToken>main</ns0:ProfileToken>
      <ns0:Velocity>
        <ns1:PanTilt xmlns:ns1="http://www.onvif.org/ver10/schema" x="-0.5" y="0"/>
      </ns0:Velocity>
    </ns0:ContinuousMove>
  </soap-env:Body>
</soap-env:Envelope>"#;

    #[test]
    fn parses_soap_request() {
        let req = parse_soap(MOVE).unwrap();
        assert_eq!(req.operation.name, "ContinuousMove");
        assert_eq!(req.operation.child("ProfileToken").unwrap().text(), "main");
        let pan_tilt = req.operation.path(&["Velocity", "PanTilt"]).unwrap();
        assert_eq!(pan_tilt.attr("x"), Some("-0.5"));
        assert_eq!(pan_tilt.attr("y"), Some("0"));
    }

    #[test]
    fn verifies_password_digest() {
        let req = parse_soap(MOVE).unwrap();
        let token = UsernameToken::from_header(req.header.as_ref().unwrap()).unwrap();
        assert_eq!(token.username, "frigate");
        assert!(token.verify("secret"));
        assert!(!token.verify("wrong"));
    }

    #[test]
    fn verifies_password_text() {
        let xml = MOVE
            .replace("#PasswordDigest", "#PasswordText")
            .replace("vswM0WR7AJAjFucTHkHFQT/hgtg=", "secret");
        let req = parse_soap(&xml).unwrap();
        let token = UsernameToken::from_header(req.header.as_ref().unwrap()).unwrap();
        assert!(token.verify("secret"));
        assert!(!token.verify("secret2"));
    }

    #[test]
    fn rejects_bad_xml() {
        assert!(parse_soap("<a><b></a>").is_err());
        assert!(parse_soap("<Envelope><Body></Body></Envelope>").is_err());
        assert!(parse_soap("<NotSoap/>").is_err());
        let deep = "<a>".repeat(100) + &"</a>".repeat(100);
        assert!(parse_xml(&deep).is_err());
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("PT10S"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("PT1.5S"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("PT1M30S"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("PT1H"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("P1D"), None);
        assert_eq!(parse_duration("PT5"), None);
        assert_eq!(parse_duration("garbage"), None);
    }

    #[test]
    fn formats_datetime() {
        let t = time::OffsetDateTime::from_unix_timestamp(1_794_355_200).unwrap();
        assert_eq!(xs_datetime(t), "2026-11-11T00:00:00Z");
    }

    #[test]
    fn parses_http_head() {
        let head = parse_http_head(
            "POST /onvif/ptz_service HTTP/1.1\r\nHost: 10.0.0.2:8001\r\nContent-Type: application/soap+xml\r\ncontent-length: 512",
        )
        .unwrap();
        assert_eq!(
            head,
            HttpHead {
                method: "POST".into(),
                path: "/onvif/ptz_service".into(),
                host: Some("10.0.0.2:8001".into()),
                content_length: Some(512),
                chunked: false,
            }
        );
        assert!(parse_http_head("POST / HTTP/1.1\r\nContent-Length: x").is_err());
    }

    #[test]
    fn escapes_fault_reason() {
        assert!(fault("s:Sender", "ter:InvalidArgVal", "<bad>").contains("&lt;bad&gt;"));
    }
}
