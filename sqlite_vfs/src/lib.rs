pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

tonic::include_proto!("grpc_vfs");

#[cfg(test)]
mod tests {
    use crate::{grpsq_lite_client::GrpsqLiteClient, GetCapabilitiesRequest};
    use tonic::Request;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);


        // test creating the client
        let mut client = GrpsqLiteClient::connect("http://localhost:50051").await.unwrap();
        client.get_capabilities(Request::new(GetCapabilitiesRequest {
            client_token: "".to_string(),
            file_name: "".to_string(),
            readonly: false,
        }))
        .await
        .unwrap();
    }
}
