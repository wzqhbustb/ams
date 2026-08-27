//! TCP accept loop and thread-per-connection management (§7.3).
//!
//! `std::net::TcpListener` + one `std` thread per connection + a shared
//! `Arc<Engine>` — no async runtime anywhere in the workspace (the WAL
//! writer and deadlock detector are plain std threads too, and `exec` is a
//! blocking call, so an async wrapper would only pay `spawn_blocking`
//! overhead for nothing).
//!
//! Connection threads are detached: the process is the lifecycle owner
//! (same model as the rest of the workspace). A session failure (I/O or
//! protocol violation) is logged and closes that connection only — the
//! accept loop and all other connections are unaffected.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::thread;

use pg_engine::Engine;

use crate::error::Result;
use crate::session::Session;

/// A bound wire server over a shared engine.
pub struct Server {
    listener: TcpListener,
    engine: Arc<Engine>,
}

impl Server {
    /// Bind `addr` (e.g. `"127.0.0.1:5432"`; port 0 picks a free port).
    pub fn bind<A: ToSocketAddrs>(addr: A, engine: Arc<Engine>) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self { listener, engine })
    }

    /// The address the listener is bound to (useful with port 0).
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// The shared engine (tests use it for direct typed-API fixtures).
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Accept connections forever, one std thread per connection. Returns
    /// only if `accept` itself fails.
    pub fn serve(self) -> Result<()> {
        loop {
            let (stream, peer) = self.listener.accept()?;
            // Small write-combining buffers are built per response batch;
            // Nagle would only add latency to the request/response pattern.
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!(error = %e, %peer, "set_nodelay failed");
            }
            let engine = Arc::clone(&self.engine);
            let spawn = thread::Builder::new()
                .name(format!("pg-wire conn {peer}"))
                .spawn(move || {
                    let mut session = Session::new(engine);
                    if let Err(e) = session.run(&stream) {
                        // Connection-local failure: the client sees a closed
                        // socket, the server keeps serving everyone else.
                        tracing::warn!(error = %e, %peer, "pg-wire session ended with error");
                    }
                });
            if let Err(e) = spawn {
                tracing::warn!(error = %e, %peer, "failed to spawn connection thread");
            }
        }
    }
}
