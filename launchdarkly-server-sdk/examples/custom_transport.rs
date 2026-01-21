use bytes::Bytes;
use http::Request;
use launchdarkly_server_sdk::{
    ConfigBuilder, EventProcessorBuilder, HttpTransport, ResponseFuture,
};
use std::time::Instant;

/// Example of a custom transport that wraps another transport and adds logging.
///
/// This demonstrates how to implement the HttpTransport trait to add middleware
/// functionality like logging, metrics, retries, circuit breakers, etc.
#[derive(Clone)]
struct LoggingTransport<T: HttpTransport> {
    inner: T,
}

impl<T: HttpTransport> LoggingTransport<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: HttpTransport> HttpTransport for LoggingTransport<T> {
    fn request(&self, request: Request<Bytes>) -> ResponseFuture {
        let method = request.method().clone();
        let uri = request.uri().clone();
        let start = Instant::now();

        println!("[REQUEST] {method} {uri}");

        let inner = self.inner.clone();
        Box::pin(async move {
            let result = inner.request(request).await;
            let elapsed = start.elapsed();

            match &result {
                Ok(response) => {
                    println!(
                        "[RESPONSE] {} {} - Status: {} - Duration: {:?}",
                        method,
                        uri,
                        response.status(),
                        elapsed
                    );
                }
                Err(e) => {
                    println!("[ERROR] {method} {uri} - Error: {e} - Duration: {elapsed:?}");
                }
            }

            result
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get SDK key from environment
    let sdk_key =
        std::env::var("LAUNCHDARKLY_SDK_KEY").unwrap_or_else(|_| "your-sdk-key".to_string());

    if sdk_key == "your-sdk-key" {
        eprintln!("Please set LAUNCHDARKLY_SDK_KEY environment variable");
        std::process::exit(1);
    }

    // Create the base HTTPS transport
    let base_transport = launchdarkly_server_sdk::HyperTransport::new_https();

    // Wrap it with logging middleware
    let logging_transport = LoggingTransport::new(base_transport);

    // Configure the SDK to use the custom transport
    let config = ConfigBuilder::new(&sdk_key)
        .event_processor(
            EventProcessorBuilder::new()
                .transport(logging_transport.clone())
                .flush_interval(std::time::Duration::from_secs(5)),
        )
        .build()?;

    // Create the client - you'll see all HTTP requests logged
    println!("Initializing LaunchDarkly client with logging transport...");
    let client = launchdarkly_server_sdk::Client::build(config)?;
    client.start_with_default_executor();

    // Wait for initialization
    println!("Waiting for client initialization...");
    match client
        .wait_for_initialization(std::time::Duration::from_secs(10))
        .await
    {
        Some(true) => {
            println!("Client initialized successfully!");

            // Evaluate a flag (will trigger HTTP events)
            let context = launchdarkly_server_sdk::ContextBuilder::new("example-user-key")
                .build()
                .expect("Failed to create context");

            let flag_value = client.bool_variation(&context, "example-flag", false);
            println!("Flag 'example-flag' evaluated to: {flag_value}");

            // Wait a bit to see event flushing
            println!("Waiting to observe event flushing...");
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        }
        Some(false) => {
            eprintln!("Client failed to initialize");
        }
        None => {
            eprintln!("Client initialization timed out");
        }
    }

    // Shutdown the client
    println!("Shutting down client...");
    client.close();

    Ok(())
}
