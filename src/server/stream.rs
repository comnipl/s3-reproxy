use aws_sdk_s3::primitives::{ByteStream, SdkBody};

//https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/primitives/struct.SdkBody.html#method.from_body_1_x
async fn make_byte_stream(stream: ByteStream) {}
