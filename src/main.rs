//! All about 'Pin' in Rust

//! we can call tokio::main with a different flavour, if we want
//! #[tokio::main(flavor = "current_thread")]
//! ^ that is make sure that tokio only uses a single thread at once

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    executor_blocking().await;
    non_block_executor().await;
    run_my_fut().await;
    run_ft_3().await;
    do_slow_read().await?;

    Ok(())
}

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

// even though this function is 'async', it's still blocking the executor
async fn executor_blocking() {
    println!("hello...");
    // this sleep is a syscall to block the executor
    std::thread::sleep(Duration::from_millis(500));
    println!("...goodbye");
}

async fn non_block_executor() {
    println!("hello...");
    // this sleep is a Future, registers a timer when first polled
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("...goodbye");
}

// Let's make our own Future type!

use std::future::Future;
struct MyFuture {
    slept: bool,
}

impl MyFuture {
    fn new() -> Self {
        Self { slept: false }
    }
}

async fn run_my_fut() {
    let fut = MyFuture::new();
    fut.await;
}

impl Future for MyFuture {
    type Output = ();

    // futures get polled by the system
    // Poll::Ready(()) when the future is ready
    //
    // `poll` is not called in a loop!
    // (imagine if there's a slow network connection at the other end...)
    // Futures are only awakened when something actually happens
    //    -> we use the `cx` context variable to inform when it should re-poll!
    //
    // if we want to modify `self`, we can just take a `mut self` instead
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("MyFuture::poll()");

        match self.slept {
            false => {
                let waker = cx.waker().clone();
                // kind of wasteful to spin up a new thread every time we want to wait...
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_secs(1));
                    // only wake after a second has passed!
                    waker.wake();
                });
                self.slept = true;
                Poll::Pending
            }
            true => Poll::Ready(()),
        }
    }
}

// Wasteful to spin up a thread each time we want to wait!!
// We can use tokio:::time::sleep, which is more efficient

use tokio::time::Sleep;

struct MyFuture2 {
    sleep: Sleep,
}

impl MyFuture2 {
    fn new() -> Self {
        Self {
            sleep: tokio::time::sleep(Duration::from_secs(1)),
        }
    }
}

impl Future for MyFuture2 {
    type Output = ();

    // the reciever for `self` is actually a `Pin`
    // therefore, we can't access some things on some methods
    // what the hell is a pin???
    //
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("MyFuture2::poll()");
        Poll::Ready(())
        // we'd ideally like to do this:
        // self.sleep.poll(cx)
        //
        // ...but can't because of a compiler error
    }
}

// OK, Pin<T> is a thing, let's try with that to avoid a compiler error now!

async fn run_ft_3() {
    let fut = MyFuture3::new();
    fut.await;
}

struct MyFuture3 {
    sleep: Pin<Box<Sleep>>,
}

impl MyFuture3 {
    fn new() -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::from_secs(1))),
        }
    }
}

impl Future for MyFuture3 {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("MyFuture3::poll()");
        self.sleep.as_mut().poll(cx)
    }
}

// Why Pin?
// as of Rust 1.51, Traits cannot have async functions.

use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    time::Instant,
};

async fn do_slow_read() -> Result<(), tokio::io::Error> {
    let mut buf = vec![0u8; 128 * 1024];

    println!("FAST READ");
    let mut f = File::open("/dev/urandom").await?;
    let before = Instant::now();
    f.read_exact(&mut buf).await?;
    println!("Read {} bytes in {:?}", buf.len(), before.elapsed());

    println!("SLOW READ");
    let mut f = SlowRead::new(File::open("/dev/urandom").await?);
    let before = Instant::now();
    f.read_exact(&mut buf).await?;
    println!("Read {} bytes in {:?}", buf.len(), before.elapsed());

    Ok(())
}

struct SlowRead<R> {
    reader: Pin<Box<R>>,
    sleep: Pin<Box<Sleep>>,
}

impl<R> SlowRead<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: Box::pin(reader),
            sleep: Box::pin(tokio::time::sleep(Default::default())),
        }
    }
}

impl<R> AsyncRead for SlowRead<R>
where
    R: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // poll the sleeper...
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => {
                // whenever `sleep` completes, reset it...
                self.sleep
                    .as_mut()
                    .reset(Instant::now() + Duration::from_millis(25));
                // poll the inner reader
                self.reader.as_mut().poll_read(cx, buf)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Rules for Pin<T>:
//   - Once we _pin_ something, i.e. once we construct a `Pin<&mut T>` of it, we can
//   _never_ use it unpinned (i.e. as `&mut T`) ever again, unless it implements `Unpin`.

// as long as we follow these rules, we don't need to Box pins.
// `Self: Unpin` is implied, as long as all it's fields are Unpin

// In our example, we constrain R to be `Unpin`, but `Sleep` is NOT `Unpin`.
// Therefore, `Self: !Unpin`

struct SlowRead2<R> {
    reader: R,
    sleep: Sleep,
}

impl<R> SlowRead2<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            sleep: tokio::time::sleep(Default::default()),
        }
    }
}

impl<R> AsyncRead for SlowRead2<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // pin-project both fields
        //
        // as long as we never use either field unpinned, there will be no undefined behaviour, and
        // we're good to go, so don't need to worry about the `unsafe` block
        let (mut sleep, reader) = unsafe {
            let this = self.get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.sleep),
                Pin::new_unchecked(&mut this.reader),
            )
        };

        match sleep.as_mut().poll(cx) {
            Poll::Ready(_) => {
                sleep.reset(Instant::now() + Duration::from_millis(25));
                reader.poll_read(cx, buf)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// So using `unsafe` blocks is fine as long as we maintain the invariants of the pinning system.

// So, in summary, Pins are used to point to something as a static location, because internal
// timers, schedulers, executors etc. might need to point to a given object over time, and we
// cannot afford for the memory address to change.
//
// tokio might poll a given object at a later time, so we need a static address to refer to it, and
// that is what Pin provides.
// This is why once we construct a Pin<&mut T> of something, we can never use it unpinned.
// It's because the Pin provides a canonial, static reference to the underlying thing, which the
// polling stuff in Futures needs to be static.
