use rand::rngs::OsRng;
use rsa::pkcs8::EncodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::{RsaPrivateKey, pkcs1v15::SigningKey};

pub struct Signer {
    signing_key: SigningKey<Sha256>,
    cert_pem: String,
}

impl Signer {
    pub fn generate() -> Self {
        let mut rng = OsRng;
        let private_key =
            RsaPrivateKey::new(&mut rng, 2048).expect("failed to generate RSA-2048 key");

        // Serialize private key as PKCS#8 PEM so rcgen can consume it
        let pkcs8_pem = private_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .expect("pkcs8 pem encoding");

        let key_pair =
            rcgen::KeyPair::from_pem(pkcs8_pem.as_str()).expect("rcgen key pair from pkcs8");

        let params = rcgen::CertificateParams::new(vec![]).expect("cert params");
        let cert = params.self_signed(&key_pair).expect("self-signed cert");
        let cert_pem = cert.pem();

        let signing_key = SigningKey::<Sha256>::new(private_key);
        Self {
            signing_key,
            cert_pem,
        }
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// Sign an SNS notification. Returns base64-encoded RSA-SHA256 signature.
    /// Signed string follows the SNS v2 spec: sorted field name/value pairs,
    /// each terminated by a newline.
    pub fn sign_notification(
        &self,
        topic_arn: &str,
        message_id: &str,
        message: &str,
        timestamp: &str,
    ) -> String {
        let string_to_sign = format!(
            "Message\n{message}\nMessageId\n{message_id}\nTimestamp\n{timestamp}\nTopicArn\n{topic_arn}\nType\nNotification\n"
        );
        let mut rng = OsRng;
        let sig = self
            .signing_key
            .sign_with_rng(&mut rng, string_to_sign.as_bytes());
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
    }
}
