mod common;

use iroh_blobs::BlobFormat;
use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tempfile::TempDir;

#[tokio::test]
async fn import_file_preserves_filename_in_tag() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "report.pdf", b"pdf content").await;

    let (hash, _format) = node.node.import_file(&file).await.unwrap();

    let blobs = node.node.list_blobs(None, None).await.unwrap();
    let entry = blobs.into_iter().find(|e| e.hash == hash).unwrap();
    assert_eq!(entry.name, "report.pdf");

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
    let entry = blobs.into_iter().find(|e| e.hash == hash).unwrap();
    assert_eq!(entry.name, "my_dataset");

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

    let entry = node
        .node
        .list_blobs(None, None)
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.hash == hash)
        .unwrap();
    let ticket = node.node.make_ticket(hash, entry.format, Some(entry.name));
    assert_eq!(ticket.name.as_deref(), Some("video.mp4"));

    node.shutdown().await;
}

#[tokio::test]
async fn blob_list_entry_for_file_shows_format_and_size() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let content = b"hello world";
    let file = common::write_file(src.path(), "test.txt", content).await;

    let (hash, _) = node.node.import_file(&file).await.unwrap();

    let entry = node
        .node
        .list_blobs(None, None)
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.hash == hash)
        .unwrap();
    assert_eq!(entry.format, BlobFormat::Raw);
    assert_eq!(entry.file_count, None);
    assert_eq!(entry.total_size, Some(content.len() as u64));

    node.shutdown().await;
}

#[tokio::test]
async fn blob_list_entry_for_directory_shows_file_count_and_total_size() {
    let node = common::TestNode::start().await;
    let src = TempDir::new().unwrap();
    let dir = src.path().join("mydir");
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let content_a = b"aaa";
    let content_b = b"bbbb";
    common::write_file(&dir, "a.txt", content_a).await;
    common::write_file(&dir, "b.txt", content_b).await;

    let (hash, _) = node.node.import_directory(&dir).await.unwrap();

    let entry = node
        .node
        .list_blobs(None, None)
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.hash == hash)
        .unwrap();
    assert_eq!(entry.format, BlobFormat::HashSeq);
    assert_eq!(entry.file_count, Some(2));
    assert_eq!(
        entry.total_size,
        Some((content_a.len() + content_b.len()) as u64)
    );

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
