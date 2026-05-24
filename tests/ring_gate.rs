mod common;

use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tempfile::TempDir;

#[tokio::test]
async fn open_ring_allows_any_peer() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "hello.txt", b"hello from ringdrop").await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME, &[Permission::Read])
        .unwrap();

    let ticket = sender
        .node
        .make_ticket(hash, format, Some("hello.txt".into()));
    let dest = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest.path()).await.unwrap();

    let got = tokio::fs::read(dest.path().join("hello.txt"))
        .await
        .unwrap();
    assert_eq!(got, b"hello from ringdrop");

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn private_ring_allows_member() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "secret.txt", b"for members only").await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender.node.registry.create_ring("friends").unwrap();
    sender
        .node
        .registry
        .add_peer_to_ring("friends", receiver.node.endpoint.id(), None)
        .unwrap();
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), "friends", &[Permission::Read])
        .unwrap();

    let ticket = sender
        .node
        .make_ticket(hash, format, Some("secret.txt".into()));
    let dest = TempDir::new().unwrap();
    receiver.node.download(&ticket, dest.path()).await.unwrap();

    let got = tokio::fs::read(dest.path().join("secret.txt"))
        .await
        .unwrap();
    assert_eq!(got, b"for members only");

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn private_ring_denies_non_member() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "private.txt", b"exclusive").await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    sender.node.registry.create_ring("vip").unwrap();
    // receiver deliberately NOT added to "vip"
    sender
        .node
        .registry
        .add_ring_to_resource(*hash.as_bytes(), "vip", &[Permission::Read])
        .unwrap();

    let ticket = sender.node.make_ticket(hash, format, None);
    let dest = TempDir::new().unwrap();

    let err = receiver
        .node
        .download(&ticket, dest.path())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("access denied"),
        "unexpected error: {err}"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn untagged_blob_is_denied() {
    let sender = common::TestNode::start().await;
    let receiver = common::TestNode::start().await;

    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "data.txt", b"some data").await;

    let (hash, format) = sender.node.import_file(&file).await.unwrap();
    // no add_ring_to_resource — fail-closed default

    let ticket = sender.node.make_ticket(hash, format, None);
    let dest = TempDir::new().unwrap();

    let err = receiver
        .node
        .download(&ticket, dest.path())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("access denied"),
        "unexpected error: {err}"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}
