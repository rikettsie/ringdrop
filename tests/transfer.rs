mod common;

use ringdrop::registry::OPEN_RING_NAME;
use tempfile::TempDir;

#[tokio::test]
async fn file_content_matches_after_transfer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let content = b"the quick brown fox jumps over the lazy dog";
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "fox.txt", content).await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender.node.registry.tag_file(hash, OPEN_RING_NAME).unwrap();

    let ticket = sender.node.make_ticket(hash, format, Some("fox.txt".into()));
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
async fn already_complete_blob_skips_transfer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let content = b"cached content";
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "cached.txt", content).await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender.node.registry.tag_file(hash, OPEN_RING_NAME).unwrap();

    let ticket = sender.node.make_ticket(hash, format, Some("cached.txt".into()));

    let dest1 = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest1.path()).await.unwrap();
    assert_eq!(
        tokio::fs::read(dest1.path().join("cached.txt")).await.unwrap(),
        content
    );

    // sender is gone — second download must succeed from local store only
    sender.shutdown().await;

    let dest2 = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest2.path()).await.unwrap();
    assert_eq!(
        tokio::fs::read(dest2.path().join("cached.txt")).await.unwrap(),
        content
    );

    receiver.shutdown().await;
}
