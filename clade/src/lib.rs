pub mod catalog {
    tonic::include_proto!("clade.catalog");
}

pub mod schema {
    tonic::include_proto!("clade.schema");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("clade_descriptor");
}
