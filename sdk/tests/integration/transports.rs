use crate::test_iggy::TestIggy;
use laser_sdk::capabilities::HelloOutcome;
use laser_sdk::prelude::full::Laser;
use laser_sdk::prelude::{ConsumerStart, ProducerMessage};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use std::path::{Path, PathBuf};
use std::time::Duration;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const REFUSED_CONNECT: Duration = Duration::from_secs(5);
// The trust store rustls reads for one process, in place of the system one.
const TRUST_STORE_FILE: &str = "SSL_CERT_FILE";
const TRUST_STORE_DIRECTORY: &str = "SSL_CERT_DIR";
// Names the endpoint for the copy of the trust store test that connects, and
// whether its trust store holds the CA that signed the server.
const VERIFIED_QUIC_ENDPOINT: &str = "LASER_TEST_VERIFIED_QUIC_ENDPOINT";
const VERIFIED_QUIC_TRUSTED: &str = "LASER_TEST_VERIFIED_QUIC_TRUSTED";
const VERIFIED_QUIC_TEST: &str = "transports::given_a_quic_server_when_the_process_trust_store_holds_its_ca_then_should_verify_and_refuse_under_another_ca";

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_server_on_quic_and_websocket_when_connected_by_scheme_then_should_publish_poll_and_reach_the_managed_probe()
 {
    let (quic, websocket) = (free_udp_port(), free_tcp_port());
    let server = TestIggy::start_with(listeners(quic, websocket, None)).await;
    for connection_string in [
        format!("iggy+quic://iggy:iggy@127.0.0.1:{quic}"),
        format!("iggy+ws://iggy:iggy@127.0.0.1:{websocket}"),
    ] {
        round_trip(&connection_string).await;
    }
    drop(server);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_tls_listeners_when_connected_then_should_verify_websocket_against_its_ca_and_quic_on_request()
 {
    let certificates = tempfile::tempdir().expect("a certificate directory");
    let tls = Certificates::issue(certificates.path());
    let (quic, websocket) = (free_udp_port(), free_tcp_port());
    let server = TestIggy::start_with(listeners(quic, websocket, Some(&tls))).await;
    let verified = format!(
        "iggy+ws://iggy:iggy@127.0.0.1:{websocket}?tls_ca_file={}&tls_domain=localhost",
        tls.ca.display()
    );
    round_trip(&verified).await;
    let foreign = format!(
        "iggy+ws://iggy:iggy@127.0.0.1:{websocket}?tls_ca_file={}&tls_domain=localhost",
        tls.other_ca.display()
    );
    assert!(
        refused(&foreign).await,
        "a WebSocket server another CA signed must be refused"
    );
    round_trip(&format!("iggy+quic://iggy:iggy@127.0.0.1:{quic}")).await;
    let untrusted = Laser::builder()
        .connection_string(format!(
            "iggy+quic://iggy:iggy@127.0.0.1:{quic}?validate_certificate=true"
        ))
        .connect_timeout(REFUSED_CONNECT)
        .build()
        .await
        .err()
        .expect("a QUIC server outside the system trust store is refused when verification is on");
    assert!(
        untrusted
            .to_string()
            .contains("operating system trust store"),
        "{untrusted}"
    );
    drop(server);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_quic_server_when_the_process_trust_store_holds_its_ca_then_should_verify_and_refuse_under_another_ca()
 {
    if let Ok(connection_string) = std::env::var(VERIFIED_QUIC_ENDPOINT) {
        return connect_verified(&connection_string).await;
    }
    let certificates = tempfile::tempdir().expect("a certificate directory");
    let tls = Certificates::issue(certificates.path());
    let (quic, websocket) = (free_udp_port(), free_tcp_port());
    let server = TestIggy::start_with(listeners(quic, websocket, Some(&tls))).await;
    let connection_string =
        format!("iggy+quic://iggy:iggy@127.0.0.1:{quic}?validate_certificate=true");
    // The trusted connect runs first and proves the listener is up, so the
    // refusal after it is the certificate and not a server still starting.
    assert!(
        child_passes(&tls.ca, &connection_string, true),
        "a QUIC server the trusted CA signed verifies, publishes and polls"
    );
    assert!(
        child_passes(&tls.other_ca, &connection_string, false),
        "a QUIC server another CA signed is refused"
    );
    drop(server);
}

// Runs this test again in a child process whose trust store is `ca` alone, so
// the system store and this process stay untouched.
fn child_passes(ca: &Path, connection_string: &str, trusted: bool) -> bool {
    std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([VERIFIED_QUIC_TEST, "--exact", "--nocapture"])
        .env(TRUST_STORE_FILE, ca)
        .env_remove(TRUST_STORE_DIRECTORY)
        .env(VERIFIED_QUIC_ENDPOINT, connection_string)
        .env(VERIFIED_QUIC_TRUSTED, trusted.to_string())
        .status()
        .expect("the child test runs")
        .success()
}

// The child half: a verified connect under the trust store its environment
// names. Only the refused connect is short, so a slow listener never looks
// like a refused certificate.
async fn connect_verified(connection_string: &str) {
    let trusted = std::env::var(VERIFIED_QUIC_TRUSTED).is_ok_and(|value| value == "true");
    let connected = Laser::builder()
        .connection_string(connection_string)
        .connect_timeout(if trusted {
            RECEIVE_TIMEOUT
        } else {
            REFUSED_CONNECT
        })
        .build()
        .await;
    if trusted {
        drop(connected.unwrap_or_else(|error| panic!("the trusted QUIC server connects: {error}")));
        round_trip(connection_string).await;
    } else {
        let refused = connected
            .err()
            .expect("a QUIC server another CA signed is refused");
        assert!(
            refused.to_string().contains("operating system trust store"),
            "{refused}"
        );
    }
}

async fn round_trip(connection_string: &str) {
    let laser = Laser::connect_with_stream(connection_string, "transports")
        .await
        .unwrap_or_else(|error| panic!("{connection_string} connects: {error}"));
    assert_ne!(
        laser.capabilities().await.hello,
        HelloOutcome::Failed,
        "the managed probe rides the binary request path and the server answers it"
    );
    laser
        .stream("transports")
        .ensure()
        .await
        .expect("the stream exists");
    let topic = laser.topic("records");
    topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes")
        .send_batch([ProducerMessage::new(connection_string.as_bytes().to_vec())])
        .await
        .expect("the record publishes");
    let mut consumer = topic
        .consumer("transport-reader", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("the consumer builds");
    let mut payloads = Vec::new();
    while !payloads.contains(&connection_string.as_bytes().to_vec()) {
        let message = consumer
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("the record is polled back");
        payloads.push(message.payload.to_vec());
    }
    consumer.shutdown().await.expect("the consumer shuts down");
}

async fn refused(connection_string: &str) -> bool {
    Laser::builder()
        .connection_string(connection_string)
        .connect_timeout(REFUSED_CONNECT)
        .build()
        .await
        .is_err()
}

fn listeners(quic: u16, websocket: u16, tls: Option<&Certificates>) -> Vec<(String, String)> {
    let mut settings = vec![
        ("IGGY_QUIC_ENABLED".to_owned(), "true".to_owned()),
        ("IGGY_QUIC_ADDRESS".to_owned(), format!("127.0.0.1:{quic}")),
        ("IGGY_WEBSOCKET_ENABLED".to_owned(), "true".to_owned()),
        (
            "IGGY_WEBSOCKET_ADDRESS".to_owned(),
            format!("127.0.0.1:{websocket}"),
        ),
    ];
    if let Some(tls) = tls {
        let (cert, key) = (
            tls.server.display().to_string(),
            tls.key.display().to_string(),
        );
        settings.extend([
            (
                "IGGY_QUIC_CERTIFICATE_SELF_SIGNED".to_owned(),
                "false".to_owned(),
            ),
            ("IGGY_QUIC_CERTIFICATE_CERT_FILE".to_owned(), cert.clone()),
            ("IGGY_QUIC_CERTIFICATE_KEY_FILE".to_owned(), key.clone()),
            ("IGGY_WEBSOCKET_TLS_ENABLED".to_owned(), "true".to_owned()),
            (
                "IGGY_WEBSOCKET_TLS_SELF_SIGNED".to_owned(),
                "false".to_owned(),
            ),
            ("IGGY_WEBSOCKET_TLS_CERT_FILE".to_owned(), cert),
            ("IGGY_WEBSOCKET_TLS_KEY_FILE".to_owned(), key),
        ]);
    }
    settings
}

// A test CA, a server certificate it signs for `localhost` and 127.0.0.1, and
// an unrelated CA, written as PEM files.
struct Certificates {
    ca: PathBuf,
    other_ca: PathBuf,
    server: PathBuf,
    key: PathBuf,
}

impl Certificates {
    fn issue(directory: &Path) -> Self {
        let authority = || {
            let key = KeyPair::generate().expect("a CA key");
            let mut params = CertificateParams::new(Vec::new()).expect("CA parameters");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let certificate = params.clone().self_signed(&key).expect("a CA certificate");
            (certificate, Issuer::new(params, key))
        };
        let (ca, issuer) = authority();
        let (other_ca, _) = authority();
        let server_key = KeyPair::generate().expect("a server key");
        let server = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("server parameters")
            .signed_by(&server_key, &issuer)
            .expect("a server certificate");
        let write = |name: &str, pem: String| {
            let path = directory.join(name);
            std::fs::write(&path, pem).expect("the PEM file writes");
            path
        };
        Self {
            ca: write("ca.pem", ca.pem()),
            other_ca: write("other-ca.pem", other_ca.pem()),
            server: write("server.pem", server.pem()),
            key: write("server-key.pem", server_key.serialize_pem()),
        }
    }
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free TCP port")
        .port()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .expect("a free UDP port")
        .port()
}
