pub mod schema {
    tonic::include_proto!("clade.schema");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("clade_descriptor");
}

pub mod sync {
    use arrow_flight::sql::{Any, ProstMessageExt};
    use prost::Message;

    tonic::include_proto!("clade.sync");

    // We need this since the native arrow-flight `do_put` mechanism relies on
    // decoding the command into `Any` prior to running it:
    // https://github.com/apache/arrow-rs/blob/a61f1dc8ea132add731e4426e11d341a1de5ca92/arrow-flight/src/sql/server.rs#L705-L706
    //
    // This in turn allows us to circumvent the default `do_put`, and use our
    // custom message/fields in `do_put_fallback` to convey additional information
    // about the command.
    impl ProstMessageExt for DataSyncCommand {
        fn type_url() -> &'static str {
            "DataSyncCommand"
        }

        fn as_any(&self) -> Any {
            Any {
                type_url: "DataSyncCommand".to_string(),
                value: self.encode_to_vec().into(),
            }
        }
    }
}
