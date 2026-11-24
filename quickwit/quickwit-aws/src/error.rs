// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::match_like_matches_macro)]

use aws_runtime::retries::classifiers::{THROTTLING_ERRORS, TRANSIENT_ERRORS};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_smithy_types::retry::ErrorKind;

use crate::retry::AwsRetryable;

/// Extracts the `x-amz-retry-after` header from an SDK error response, if present.
/// The header value is in milliseconds.
pub fn retry_after_from_sdk_error<E>(error: &SdkError<E>) -> Option<std::time::Duration> {
    let headers = match error {
        SdkError::ServiceError(e) => e.raw().headers(),
        SdkError::ResponseError(e) => e.raw().headers(),
        _ => return None,
    };
    headers
        .get("x-amz-retry-after")
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
}

impl<E> AwsRetryable for SdkError<E>
where E: AwsRetryable
{
    fn is_retryable(&self) -> bool {
        match self {
            SdkError::ConstructionFailure(_) => false,
            SdkError::TimeoutError(_) => true,
            SdkError::DispatchFailure(failure) => {
                failure.is_io()
                    || failure.is_timeout()
                    || matches!(
                        failure.as_other(),
                        Some(
                            ErrorKind::TransientError
                                | ErrorKind::ThrottlingError
                                | ErrorKind::ServerError
                        )
                    )
            }
            SdkError::ResponseError(_) => true,
            SdkError::ServiceError(error) => error.err().is_retryable(),
            _ => false,
        }
    }

    fn retry_after(&self) -> Option<std::time::Duration> {
        retry_after_from_sdk_error(self)
    }
}

fn is_retryable(meta: &aws_sdk_s3::error::ErrorMetadata) -> bool {
    if let Some(code) = meta.code() {
        THROTTLING_ERRORS.contains(&code)
            || TRANSIENT_ERRORS.contains(&code)
            || code == "InternalError" // this is somehow not considered transient, despite the
    // associated error message containing "Please try again."
    } else {
        false
    }
}

impl AwsRetryable for GetObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for DeleteObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for DeleteObjectsError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for UploadPartError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for CompleteMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for AbortMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for CreateMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for PutObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for HeadObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for ListObjectsV2Error {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

#[cfg(feature = "sqs")]
mod sqs {
    use aws_sdk_sqs::operation::change_message_visibility::ChangeMessageVisibilityError;
    use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError;
    use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;

    use super::*;

    impl AwsRetryable for ReceiveMessageError {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    impl AwsRetryable for DeleteMessageBatchError {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    impl AwsRetryable for ChangeMessageVisibilityError {
        fn is_retryable(&self) -> bool {
            false
        }
    }
}
