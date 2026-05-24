mod common;

use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tempfile::TempDir;

#[tokio::test]
async fn file_content_matches_after_transfer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let content = b"the quick brown fox jumps over the lazy dog";
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "fox.txt", content).await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME, &[Permission::Read])
        .unwrap();

    let ticket = sender
        .node
        .make_ticket(hash, format, Some("fox.txt".into()));
    let dest = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest.path()).await.unwrap();

    assert_eq!(
        tokio::fs::read(dest.path().join("fox.txt")).await.unwrap(),
        content
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn directory_contents_match_after_transfer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let src = TempDir::new().unwrap();
    let dir = src.path().join("mydir");
    tokio::fs::create_dir_all(&dir).await.unwrap();
    common::write_file(&dir, "hello.txt", b"hello world").await;
    common::write_file(&dir, "data.bin", b"\x00\x01\x02\x03").await;

    let (hash, format) = sender.node.import_directory(&dir).await.unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME, &[Permission::Read])
        .unwrap();

    let ticket = sender.node.make_ticket(hash, format, Some("mydir".into()));
    let dest = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest.path()).await.unwrap();

    let out = dest.path().join("mydir");
    assert_eq!(
        tokio::fs::read(out.join("hello.txt")).await.unwrap(),
        b"hello world"
    );
    assert_eq!(
        tokio::fs::read(out.join("data.bin")).await.unwrap(),
        b"\x00\x01\x02\x03"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn already_complete_blob_skips_transfer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let content = b"cached content";
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "cached.txt", content).await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME, &[Permission::Read])
        .unwrap();

    let ticket = sender
        .node
        .make_ticket(hash, format, Some("cached.txt".into()));

    let dest1 = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest1.path()).await.unwrap();
    assert_eq!(
        tokio::fs::read(dest1.path().join("cached.txt"))
            .await
            .unwrap(),
        content
    );

    // sender is gone — second download must succeed from local store only
    sender.shutdown().await;

    let dest2 = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest2.path()).await.unwrap();
    assert_eq!(
        tokio::fs::read(dest2.path().join("cached.txt"))
            .await
            .unwrap(),
        content
    );

    receiver.shutdown().await;
}
