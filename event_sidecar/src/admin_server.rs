use anyhow::Error;
use hyper::Server;
use metrics::metrics_summary;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    process::ExitCode,
    time::Duration,
};
use tower::{ServiceBuilder, buffer::Buffer, make::Shared};
use tracing::info;
use warp::{Filter, Rejection, Reply};

use crate::{
    types::config::AdminApiServerConfig,
    utils::{BindTarget, Unexpected, bind_tcp_listener, root_filter},
};

const BIND_ALL_INTERFACES: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
struct AdminServer {
    bind_target: BindTarget,
    max_concurrent_requests: u32,
    max_requests_per_second: u32,
}

impl AdminServer {
    pub async fn start(self) -> Result<(), Error> {
        let api = root_filter().or(metrics_filter());
        let (listener, listening_address) =
            bind_tcp_listener(self.bind_target).map_err(|error| Error::msg(error.to_string()))?;

        let warp_service = warp::service(api);
        let tower_service = ServiceBuilder::new()
            .concurrency_limit(self.max_concurrent_requests as usize)
            .rate_limit(
                u64::from(self.max_requests_per_second),
                Duration::from_secs(1),
            )
            .service(warp_service);
        info!(address = %listening_address, "started Admin API server");
        Server::from_tcp(listener)?
            .serve(Shared::new(Buffer::new(tower_service, 50)))
            .await?;

        Err(Error::msg("Admin server shutting down"))
    }
}

pub async fn run_server(config: AdminApiServerConfig) -> Result<ExitCode, Error> {
    run_server_with_inherited_listener(config, None).await
}

pub async fn run_server_with_inherited_listener(
    config: AdminApiServerConfig,
    inherited_listener: Option<TcpListener>,
) -> Result<ExitCode, Error> {
    if config.enable_server {
        let bind_target = if let Some(listener) = inherited_listener {
            BindTarget::Listener(listener)
        } else {
            BindTarget::SocketAddr(SocketAddr::new(BIND_ALL_INTERFACES, config.port))
        };
        AdminServer {
            bind_target,
            max_concurrent_requests: config.max_concurrent_requests,
            max_requests_per_second: config.max_requests_per_second,
        }
        .start()
        .await
        .map(|()| ExitCode::SUCCESS)
    } else {
        info!("Admin API server is disabled. Skipping...");
        Ok(ExitCode::SUCCESS)
    }
}

/// Return metrics data at a given time.
/// Return: prometheus-formatted metrics data.
/// Example: curl http://127.0.0.1:18887/metrics
fn metrics_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("metrics")
        .and(warp::get())
        .and_then(metrics_handler)
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let res_custom = metrics_summary()
        .map_err(|err| warp::reject::custom(Unexpected(Error::msg(err.to_string()))))?;

    Ok(res_custom)
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, time::Duration};

    use crate::{
        admin_server::{run_server, run_server_with_inherited_listener},
        types::config::AdminApiServerConfig,
    };
    use metrics::observe_error;
    use portpicker::pick_unused_port;
    use reqwest::Response;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_helper_without_inherited_listener_should_bind_from_config() {
        let port = pick_unused_port().unwrap();
        let request_url = format!("http://localhost:{port}/metrics");
        let admin_config = AdminApiServerConfig {
            enable_server: true,
            port,
            max_concurrent_requests: 1,
            max_requests_per_second: 1,
        };
        observe_error("admin_server_test", "fallback_bind");
        tokio::spawn(run_server_with_inherited_listener(admin_config, None));

        let response = fetch_metrics_data(&request_url).await;
        let text = response.text().await.unwrap();
        assert!(!text.trim().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_config_should_start_admin_server() {
        let port = pick_unused_port().unwrap();
        let request_url = format!("http://localhost:{port}/metrics");
        let admin_config = AdminApiServerConfig {
            enable_server: true,
            port,
            max_concurrent_requests: 1,
            max_requests_per_second: 1,
        };
        observe_error("admin_server_test", "config_bind");
        tokio::spawn(run_server(admin_config));

        let response = fetch_metrics_data(&request_url).await;
        let text = response.text().await.unwrap();
        assert!(!text.trim().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_inherited_listener_should_start_admin_server() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let listening_address = listener.local_addr().unwrap();
        let request_url = format!("http://{listening_address}/metrics");
        let admin_config = AdminApiServerConfig {
            enable_server: true,
            port: pick_unused_port().unwrap(),
            max_concurrent_requests: 1,
            max_requests_per_second: 1,
        };
        observe_error("admin_server_test", "inherited_bind");
        tokio::spawn(run_server_with_inherited_listener(
            admin_config,
            Some(listener),
        ));

        let response = fetch_metrics_data(&request_url).await;
        let text = response.text().await.unwrap();
        assert!(!text.trim().is_empty());
    }

    async fn fetch_metrics_data(request_url: &str) -> Response {
        let client = reqwest::Client::new();
        for _ in 0..20 {
            if let Ok(response) = client.get(request_url).send().await {
                if response.status().is_success() {
                    return response;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("Error requesting the /metrics endpoint");
    }
}
