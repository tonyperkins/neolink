/// Video streams encapsulate a stream of BcMedia
#[derive(Debug, Clone)]
pub enum BcMedia {
    /// Holds info on the stream
    InfoV1(BcMediaInfoV1),
    /// Holds info on the stream
    InfoV2(BcMediaInfoV2),
    /// Holds an IFrame either H264 or H265
    Iframe(BcMediaIframe),
    /// Holds a PFrame either H264 or H265
    Pframe(BcMediaPframe),
    /// Holds AAC audio
    Aac(BcMediaAac),
    /// Holds ADPCM audio
    Adpcm(BcMediaAdpcm),
}
//
pub(super) const MAGIC_HEADER_BCMEDIA_INFO_V1: u32 = 0x31303031;

/// The start of a BcMedia stream contains this message
/// which describes the data to follow
#[derive(Debug, Clone)]
pub struct BcMediaInfoV1 {
    // This is the size of the header so it's actually a fixed value
    // The other messages have body size here so maybe that's why
    // it's included
    // pub header_size: u32,
    /// Width of the video
    pub video_width: u32,
    /// Height of the video
    pub video_height: u32,
    // pub unknown: u8,
    /// Frames per second. On older cameras this seems to be an index of the FPS on a lookup table
    pub fps: u8,
    /// Start year of the stream
    pub start_year: u8,
    /// Start month of the stream
    pub start_month: u8,
    /// Start day of the stream
    pub start_day: u8,
    /// Start hour of the stream
    pub start_hour: u8,
    /// Start minute of the stream
    pub start_min: u8,
    /// Start seconds of the stream
    pub start_seconds: u8,
    /// End year of the video probably only useful for the recorded files on the SD card
    pub end_year: u8,
    /// End month of the video probably only useful for the recorded files on the SD card
    pub end_month: u8,
    /// End day of the video probably only useful for the recorded files on the SD card
    pub end_day: u8,
    /// End hour of the video probably only useful for the recorded files on the SD card
    pub end_hour: u8,
    /// End min of the video probably only useful for the recorded files on the SD card
    pub end_min: u8,
    /// End seconds of the video probably only useful for the recorded files on the SD card
    pub end_seconds: u8,
    // unknown: u16
}
//
pub(super) const MAGIC_HEADER_BCMEDIA_INFO_V2: u32 = 0x32303031;

/// The start of a BcMedia stream contains this message
/// which describes the data to follow
#[derive(Debug, Clone)]
pub struct BcMediaInfoV2 {
    // This is the size of the header so it's actually a fixed value
    // The other messages have body size here so maybe that's why
    // it's included
    // pub header_size: u32,
    /// Width of the video
    pub video_width: u32,
    /// Height of the video
    pub video_height: u32,
    // pub unknown: u8,
    /// Frames per second. On older cameras this seems to be an index of the FPS on a lookup table
    pub fps: u8,
    /// Start year of the stream
    pub start_year: u8,
    /// Start month of the stream
    pub start_month: u8,
    /// Start day of the stream
    pub start_day: u8,
    /// Start hour of the stream
    pub start_hour: u8,
    /// Start minute of the stream
    pub start_min: u8,
    /// Start seconds of the stream
    pub start_seconds: u8,
    /// End year of the video probably only useful for the recorded files on the SD card
    pub end_year: u8,
    /// End month of the video probably only useful for the recorded files on the SD card
    pub end_month: u8,
    /// End day of the video probably only useful for the recorded files on the SD card
    pub end_day: u8,
    /// End hour of the video probably only useful for the recorded files on the SD card
    pub end_hour: u8,
    /// End min of the video probably only useful for the recorded files on the SD card
    pub end_min: u8,
    /// End seconds of the video probably only useful for the recorded files on the SD card
    pub end_seconds: u8,
    // unknown: u16
}

// IFrame magics include the channel number in them
pub(super) const MAGIC_HEADER_BCMEDIA_IFRAME: u32 = 0x63643030;
pub(super) const MAGIC_HEADER_BCMEDIA_IFRAME_LAST: u32 = 0x63643039;

/// Video Types for I/PFrame
#[derive(Debug, Clone, Copy)]
pub enum VideoType {
    /// H264 video data
    H264,
    /// H265 video data
    H265,
}

/// This is a BcMedia video IFrame.
#[derive(Clone)]
pub struct BcMediaIframe {
    /// "H264", or "H265"
    pub video_type: VideoType,
    // Size of payload after header in bytes
    // pub payload_size: u32,
    // unknown: u32, // NVR channel count? Known values 1-00/08 2-00 3-00 4-00
    /// Timestamp in microseconds
    pub microseconds: u32,
    // unknown: u32, // Known values 1-00/23/5A 2-00 3-00 4-00
    /// POSIX time (seconds since 00:00:00 Jan 1 1970)
    pub time: Option<u32>,
    //unknown: u32, // Known values 1-00/06/29 2-00/01 3-00/C3 4-00
    /// Raw IFrame data
    pub data: Vec<u8>,
}

impl std::fmt::Debug for BcMediaIframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"video_type", &self.video_type)
            // .entry(&"payload_size", &self.payload_size)
            .entry(&"microseconds", &self.microseconds)
            .entry(&"time", &self.time)
            .entry(
                &"data[0..10]",
                &self.data[0..std::cmp::min(20, self.data.len())].to_vec(),
            )
            .entry(
                &"data[-10..-1]",
                &self.data[std::cmp::max(0, self.data.len() - 20)..self.data.len()].to_vec(),
            )
            .entry(&"data.len()", &self.data.len())
            .finish()
    }
}

// PFrame magics include the channel number in them
pub(super) const MAGIC_HEADER_BCMEDIA_PFRAME: u32 = 0x63643130;
pub(super) const MAGIC_HEADER_BCMEDIA_PFRAME_LAST: u32 = 0x63643139;

/// This is a BcMedia video PFrame.
#[derive(Clone)]
pub struct BcMediaPframe {
    /// "H264", or "H265"
    pub video_type: VideoType,
    // Size of payload after header in bytes
    // pub payload_size: u32,
    // unknown: u32, // NVR channel count? Known values 1-00/08 2-00 3-00 4-00
    /// Timestamp in microseconds
    pub microseconds: u32,
    // unknown: u32, // Known values 1-00/23/5A 2-00 3-00 4-00
    /// Raw PFrame data
    pub data: Vec<u8>,
}

impl std::fmt::Debug for BcMediaPframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"video_type", &self.video_type)
            // .entry(&"payload_size", &self.payload_size)
            .entry(&"microseconds", &self.microseconds)
            .entry(
                &"data[0..20]",
                &self.data[0..std::cmp::min(20, self.data.len())].to_vec(),
            )
            .entry(
                &"data[-20..-1]",
                &self.data[std::cmp::max(0, self.data.len() - 20)..self.data.len()].to_vec(),
            )
            .entry(&"data.len()", &self.data.len())
            .finish()
    }
}

pub(super) const MAGIC_HEADER_BCMEDIA_AAC: u32 = 0x62773530;

/// This contains BcMedia audio data in AAC format
#[derive(Debug, Clone)]
pub struct BcMediaAac {
    // Size of payload after header in bytes
    // pub payload_size: u16,
    // Size of payload after header in bytes exactly the same as before
    // pub payload_size_b: u16,
    /// Raw AAC data
    pub data: Vec<u8>,
}

impl BcMediaAac {
    /// Read the ADTS header to learn the duration in micro secs
    pub fn duration(&self) -> Option<u32> {
        if self.data.len() < 8 {
            // Too small for the header
            return None;
        }
        if self.data[0] != 0b11111111 {
            // Syncword incorrect
            return None;
        }
        if (self.data[1] & 0b11110000) != 0b11110000 {
            // Syncword incorrect
            return None;
        }
        let frequency_index = (self.data[2] & 0b00111100) >> 2;
        let sample_frequency = match frequency_index {
            0 => Some(96000u32),
            1 => Some(88200u32),
            2 => Some(64000u32),
            3 => Some(48000u32),
            4 => Some(44100u32),
            5 => Some(32000u32),
            6 => Some(24000u32),
            7 => Some(22050u32),
            8 => Some(16000u32),
            9 => Some(12000u32),
            10 => Some(11025u32),
            11 => Some(8000u32),
            12 => Some(7350u32),
            _ => None,
        }?;
        log::trace!("sample_frequency: {sample_frequency}");

        let frames = (self.data[6] & 0b00000011) + 1;
        log::trace!("frames: {frames}");
        let samples = frames as u32 * 1024;
        log::trace!("samples: {samples}");
        const MICROSECONDS: u32 = 1000000;
        let duration = samples * MICROSECONDS / sample_frequency;
        Some(duration)
    }

    /// Mark every ADTS frame in the payload as MPEG-4 AAC.
    ///
    /// Some cameras (e.g. the original E1) set the ADTS ID bit, which flags the
    /// stream as MPEG-2 AAC. The AAC-LC bitstream is the same in both, but RTP
    /// payloaders for RFC 3640 only accept MPEG-4, so clearing the bit lets the
    /// audio be passed through without re-encoding. Stops at the first byte
    /// that is not an ADTS frame header.
    pub fn normalize_to_mpeg4(&mut self) {
        let data = &mut self.data;
        let mut pos = 0;
        while pos + 7 <= data.len() && data[pos] == 0xFF && (data[pos + 1] & 0xF0) == 0xF0 {
            data[pos + 1] &= !0x08;
            let frame_len = (((data[pos + 3] & 0x03) as usize) << 11)
                | ((data[pos + 4] as usize) << 3)
                | ((data[pos + 5] as usize) >> 5);
            if frame_len < 7 {
                break;
            }
            pos += frame_len;
        }
    }
}

pub(super) const MAGIC_HEADER_BCMEDIA_ADPCM: u32 = 0x62773130;

pub(super) const MAGIC_HEADER_BCMEDIA_ADPCM_DATA: u16 = 0x0100;

/// This contains BcMedia audio data in ADPCM format
#[derive(Debug, Clone)]
pub struct BcMediaAdpcm {
    // Size of payload after header in bytes
    // pub payload_size: u16,
    // Size of payload after header in bytes exactly the same as before
    // pub payload_size_b: u16,
    // more_magic: MAGIC_HEADER_BCMEDIA_ADPCM_DATA
    // Adpcm sample_block_size in bytes
    //
    // These bytes (and the MAGIC_HEADER_BCMEDIA_ADPCM_DATA) are included as
    // part of the payload_size. It may be more prudent to sealise them to
    // another structure.
    // pub sample_block_size: u16,
    /// The raw adpcm data in DVI-4 layout.
    ///
    /// One `data` should contain 4 bytes of the adpcm predictor state then one block
    /// of adpcm samples
    ///
    /// To calculate the block-align size simply remove 4 from the `len()`
    pub data: Vec<u8>,
}

impl BcMediaAdpcm {
    /// The block size, this is bytes without the block header
    pub fn block_size(&self) -> u32 {
        (self.data.len() as u32).saturating_sub(4)
    }

    /// Returns duration in micro seconds;
    pub fn duration(&self) -> Option<u32> {
        // The header holds the first sample, then two samples per byte
        let samples = self.block_size() * 2 + 1;
        // Always 8000Hz for ADPCM
        const SAMPLE_FREQUENCY: u32 = 8000;
        const MICROSECONDS: u32 = 1000000;
        let duration = samples * MICROSECONDS / SAMPLE_FREQUENCY;
        Some(duration)
    }
}

#[cfg(test)]
mod aac_tests {
    use super::BcMediaAac;

    /// An ADTS header for a frame of `len` bytes (header included), 16 kHz mono.
    fn adts(len: usize, mpeg2: bool) -> Vec<u8> {
        let mut f = vec![
            0xFF,
            if mpeg2 { 0xF9 } else { 0xF1 },
            0x60, // AAC LC, 16 kHz
            0x40 | ((len >> 11) & 0x03) as u8,
            ((len >> 3) & 0xFF) as u8,
            (((len & 0x07) << 5) as u8) | 0x1F,
            0xFC,
        ];
        f.resize(len, 0xAA);
        f
    }

    #[test]
    fn mpeg2_frames_become_mpeg4() {
        let mut data = adts(20, true);
        data.extend(adts(30, true));
        let mut aac = BcMediaAac { data };
        aac.normalize_to_mpeg4();
        assert_eq!(aac.data[1], 0xF1);
        assert_eq!(aac.data[21], 0xF1);
        // Payload untouched
        assert!(aac.data[7..20].iter().all(|&b| b == 0xAA));
        assert_eq!(aac.duration(), Some(64000));
    }

    #[test]
    fn mpeg4_and_junk_left_alone() {
        let mut data = adts(20, false);
        data.extend([0x00, 0xF9, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let orig = data.clone();
        let mut aac = BcMediaAac { data };
        aac.normalize_to_mpeg4();
        assert_eq!(aac.data, orig);

        let mut short = BcMediaAac {
            data: vec![0xFF, 0xF9],
        };
        short.normalize_to_mpeg4();
        assert_eq!(short.data, vec![0xFF, 0xF9]);
    }
}

#[cfg(test)]
mod adpcm_tests {
    use super::BcMediaAdpcm;

    #[test]
    fn dvi_block_duration_counts_header_sample() {
        // 4 byte header + 508 bytes = 1017 samples at 8 kHz
        let adpcm = BcMediaAdpcm { data: vec![0; 512] };
        assert_eq!(adpcm.block_size(), 508);
        assert_eq!(adpcm.duration(), Some(1017 * 1_000_000 / 8000));
    }

    #[test]
    fn short_block_does_not_underflow() {
        let adpcm = BcMediaAdpcm { data: vec![0; 2] };
        assert_eq!(adpcm.block_size(), 0);
    }
}
