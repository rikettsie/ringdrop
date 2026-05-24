mod common;

use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tempfile::TempDir;

#[tokio::test]
async fn import_file_preserves_filename_in_tag() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "report.pdf", b"pdf content").await;

    let (hash, _format) = node.node.import_file(&file).await.unwrap();

    let blobs = node.node.list_blobs(None, None).await.unwrap();
    let (_, _, name) = blobs.into_iter().find(|(h, _, _)| *h == hash).unwrap();
    assert_eq!(name, "report.pdf");

    node.shutdown().await;
}

#[tokio::test]
async fn import_directory_preserves_dir_name_in_tag() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let named_dir = src.path().join("my_dataset");
    tokio::fs::create_dir_all(&named_dir).await.unwrap();
    common::write_file(&named_dir, "a.txt", b"aaa").await;
    common::write_file(&named_dir, "b.txt", b"bbb").await;

    let (hash, _format) = node.node.import_directory(&named_dir).await.unwrap();

    let blobs = node.node.list_blobs(None, None).await.unwrap();
    let (_, _, name) = blobs.into_iter().find(|(h, _, _)| *h == hash).unwrap();
    assert_eq!(name, "my_dataset");

    node.shutdown().await;
}

#[tokio::test]
async fn delete_blob_removes_filename_tagged_entry() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "to_delete.bin", b"delete me").await;

    let (hash, _format) = node.node.import_file(&file).await.unwrap();
    assert_eq!(node.node.list_blobs(None, None).await.unwrap().len(), 1);

    node.node.delete_blob(hash).await.unwrap();
    assert!(node.node.list_blobs(None, None).await.unwrap().is_empty());

    node.shutdown().await;
}

#[tokio::test]
async fn list_blobs_ticket_carries_original_filename() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "video.mp4", b"fake video bytes").await;

    let (hash, _format) = node.node.import_file(&file).await.unwrap();

    let (_, fmt, name) = node
        .node
        .list_blobs(None, None)
        .await
        .unwrap()
        .into_iter()
        .find(|(h, _, _)| *h == hash)
        .unwrap();
    let ticket = node.node.make_ticket(hash, fmt, Some(name));
    assert_eq!(ticket.name.as_deref(), Some("video.mp4"));

    node.shutdown().await;
}

#[tokio::test]
async fn export_without_ticket_name_uses_hash_hex_not_download() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let content = b"nameless content";
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "original.txt", content).await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME, &[Permission::Read])
        .unwrap();

    // Simulate what old `blob list` produced — ticket with no name.
    let ticket = sender.node.make_ticket(hash, format, None);
    assert!(ticket.name.is_none());

    let dest = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest.path()).await.unwrap();

    // Must be saved as <hash-hex>, not the old "download" fallback.
    let got = tokio::fs::read(dest.path().join(hash.to_string()))
        .await
        .unwrap();
    assert_eq!(got, content);

    sender.shutdown().await;
    receiver.shutdown().await;
}
