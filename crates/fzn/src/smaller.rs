//! Making an image smaller before it is kept, by the rule every face keeps to.
//!
//! It is the face's work and not the engine's. The page has the browser's codecs and draws with
//! them, and the engine built for it would carry a second set for nothing; so each face makes an
//! image smaller with what it has, the terminal with this, and the engine only checks that what
//! it is given is a JPEG or a PNG of at most 2 MiB before it seals it. The rule is the page's,
//! step for step: turned as the camera said, at most 2048 pixels on the long edge and never
//! enlarged, a PNG kept as a PNG while it comes to 1.5 MiB or less, anything else a JPEG at 82 on
//! white, at 70 if that is over 2 MiB, and refused past that. What is written is made from the
//! pixels alone, so nothing of the original's metadata, where it was taken included, is kept.

use std::io::Cursor;

use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::PngEncoder;
use image::imageops::FilterType;
use image::{ColorType, DynamicImage, ImageDecoder, ImageFormat, ImageReader, RgbImage};

use fzn_engine::image::IMAGE_MOST;

/// The long edge of an image as it is kept, at most.
const EDGE: u32 = 2048;
/// The most a PNG may come to and still be kept as one.
const PNG_MOST: usize = 1536 * 1024;
/// A JPEG's quality, and its quality again when the first is too large.
const QUALITY: u8 = 82;
const QUALITY_AGAIN: u8 = 70;

/// Why an image could not be made smaller.
#[derive(Debug, PartialEq, Eq)]
pub enum Smaller {
    /// It is not a JPEG, PNG, WebP or GIF that can be read.
    NotAnImage,
    /// It is more than 2 MiB even made smaller.
    TooLarge,
}

/// An image made smaller by the rule: a JPEG or a PNG of at most 2 MiB.
pub fn smaller(bytes: &[u8]) -> Result<Vec<u8>, Smaller> {
    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| Smaller::NotAnImage)?;
    let format = reader.format();
    if !matches!(
        format,
        Some(ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::WebP | ImageFormat::Gif)
    ) {
        return Err(Smaller::NotAnImage);
    }
    let mut decoder = reader.into_decoder().map_err(|_| Smaller::NotAnImage)?;
    // An orientation that cannot be read is no turn at all, as a browser takes it.
    let orientation = decoder.orientation().ok();
    let mut picture = DynamicImage::from_decoder(decoder).map_err(|_| Smaller::NotAnImage)?;
    if let Some(orientation) = orientation {
        picture.apply_orientation(orientation);
    }
    if picture.width().max(picture.height()) > EDGE {
        picture = picture.resize(EDGE, EDGE, FilterType::Lanczos3);
    }
    if format == Some(ImageFormat::Png)
        && let Some(png) = png(&picture)
        && png.len() <= PNG_MOST
    {
        return Ok(png);
    }
    let on_white = on_white(&picture);
    for quality in [QUALITY, QUALITY_AGAIN] {
        let jpeg = jpeg(&on_white, quality).ok_or(Smaller::NotAnImage)?;
        if jpeg.len() <= IMAGE_MOST {
            return Ok(jpeg);
        }
    }
    Err(Smaller::TooLarge)
}

/// The picture as a PNG, its transparency kept, in eight bits a channel.
fn png(picture: &DynamicImage) -> Option<Vec<u8>> {
    let eight = match picture.color() {
        ColorType::L8 | ColorType::La8 | ColorType::Rgb8 | ColorType::Rgba8 => picture.clone(),
        _ if picture.has_alpha() => DynamicImage::ImageRgba8(picture.to_rgba8()),
        _ => DynamicImage::ImageRgb8(picture.to_rgb8()),
    };
    let mut out = Vec::new();
    eight.write_with_encoder(PngEncoder::new(&mut out)).ok()?;
    Some(out)
}

/// The picture drawn over white, which is what a JPEG, having no transparency, shows.
fn on_white(picture: &DynamicImage) -> RgbImage {
    let rgba = picture.to_rgba8();
    RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
        let [r, g, b, a] = rgba.get_pixel(x, y).0;
        let over = |c: u8| ((u16::from(c) * u16::from(a) + 255 * (255 - u16::from(a))) / 255) as u8;
        image::Rgb([over(r), over(g), over(b)])
    })
}

fn jpeg(picture: &RgbImage, quality: u8) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    picture
        .write_with_encoder(JpegEncoder::new_with_quality(&mut out, quality))
        .ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fzn_engine::image::Kind;
    use image::{Rgb, Rgba, RgbaImage};

    /// Bytes that look like noise and are the same every run, which no codec makes small.
    fn noise(seed: u32) -> impl FnMut() -> u8 {
        let mut state = seed;
        move || {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        }
    }

    fn encoded(picture: &DynamicImage, format: ImageFormat) -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        picture.write_to(&mut out, format).unwrap();
        out.into_inner()
    }

    /// A garden in green and white, as a camera might give one.
    fn garden(width: u32, height: u32) -> DynamicImage {
        DynamicImage::ImageRgb8(RgbImage::from_fn(width, height, |x, y| {
            Rgb([(x % 256) as u8, 160, (y % 256) as u8])
        }))
    }

    fn read(bytes: &[u8]) -> DynamicImage {
        image::load_from_memory(bytes).unwrap()
    }

    #[test]
    fn an_image_wider_than_2048_pixels_is_made_2048_on_its_long_edge() {
        let made = smaller(&encoded(&garden(3000, 1000), ImageFormat::Jpeg)).unwrap();
        let back = read(&made);
        assert_eq!((back.width(), back.height()), (2048, 683));

        let tall = smaller(&encoded(&garden(600, 2600), ImageFormat::Jpeg)).unwrap();
        assert_eq!(read(&tall).height(), 2048);

        // One already small enough is never made larger.
        let small = smaller(&encoded(&garden(300, 200), ImageFormat::Jpeg)).unwrap();
        let back = read(&small);
        assert_eq!((back.width(), back.height()), (300, 200));
    }

    #[test]
    fn a_small_png_stays_a_png_and_a_photo_becomes_a_jpeg() {
        let plate = smaller(&encoded(&garden(800, 600), ImageFormat::Png)).unwrap();
        assert_eq!(Kind::of(&plate), Some(Kind::Png));
        let photo = smaller(&encoded(&garden(800, 600), ImageFormat::Jpeg)).unwrap();
        assert_eq!(Kind::of(&photo), Some(Kind::Jpeg));
        let webp = smaller(&encoded(&garden(80, 60), ImageFormat::WebP)).unwrap();
        assert_eq!(Kind::of(&webp), Some(Kind::Jpeg));
        let gif = smaller(&encoded(&garden(80, 60), ImageFormat::Gif)).unwrap();
        assert_eq!(Kind::of(&gif), Some(Kind::Jpeg));
    }

    #[test]
    fn an_image_made_smaller_keeps_nothing_of_the_originals_metadata() {
        let plain = encoded(&garden(300, 200), ImageFormat::Jpeg);
        let exif = b"Exif\0\0MM\0*\0\0\0\x08\0\0a garden in spring";
        let length = (exif.len() + 2) as u16;
        let mut tagged = plain[..2].to_vec();
        tagged.extend_from_slice(&[0xFF, 0xE1]);
        tagged.extend_from_slice(&length.to_be_bytes());
        tagged.extend_from_slice(exif);
        tagged.extend_from_slice(&plain[2..]);
        assert_eq!(read(&tagged).width(), 300, "the original reads");

        let made = smaller(&tagged).unwrap();
        let holds = |what: &[u8]| made.windows(what.len()).any(|w| w == what);
        assert!(!holds(b"Exif"));
        assert!(!holds(b"a garden in spring"));
    }

    #[test]
    fn a_transparent_png_too_large_for_png_is_set_on_white() {
        let mut next = noise(7);
        // The left half is clear, whatever colour lies under it; the right is noise.
        let picture = RgbaImage::from_fn(1200, 1200, |x, _| {
            let (r, g, b) = (next(), next(), next());
            Rgba([r, g, b, if x < 600 { 0 } else { 255 }])
        });
        let png = encoded(&DynamicImage::ImageRgba8(picture), ImageFormat::Png);
        assert!(png.len() > PNG_MOST);

        let made = smaller(&png).unwrap();
        assert_eq!(Kind::of(&made), Some(Kind::Jpeg));
        assert!(made.len() <= IMAGE_MOST);
        let back = read(&made).to_rgb8();
        assert_eq!((back.width(), back.height()), (1200, 1200));
        let Rgb(clear) = *back.get_pixel(100, 600);
        assert!(clear.iter().all(|&c| c >= 250), "{clear:?}");

        // A small one keeps its transparency, as a PNG.
        let clear = RgbaImage::from_pixel(40, 40, Rgba([0, 0, 0, 0]));
        let made = smaller(&encoded(&DynamicImage::ImageRgba8(clear), ImageFormat::Png)).unwrap();
        assert_eq!(Kind::of(&made), Some(Kind::Png));
        assert_eq!(read(&made).to_rgba8().get_pixel(0, 0).0[3], 0);
    }

    #[test]
    fn a_file_that_is_not_an_image_is_said_so() {
        assert_eq!(smaller(b"Boiler data plate"), Err(Smaller::NotAnImage));
        assert_eq!(smaller(b""), Err(Smaller::NotAnImage));
        // The start of a PNG and nothing after it is not an image either.
        assert_eq!(smaller(b"\x89PNG\r\n\x1a\n"), Err(Smaller::NotAnImage));
        // Nor is a kind it does not read, a bitmap say.
        assert_eq!(smaller(b"BM\0\0\0\0"), Err(Smaller::NotAnImage));
    }
}
