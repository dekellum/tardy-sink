#![warn(rust_2018_idioms)]
#![feature(async_await)]

use std::pin::Pin;
use std::task::{Context, Poll};
use futures::sink::Sink;
use pin_utils::{unsafe_pinned, unsafe_unpinned};

/// A sink which doesn't know it's readiness until it tries to consume an Item.
pub trait TardySink<Item>
{
    type SinkError;

    /// Attempt to consume an `Item`, as a logical combination of
    /// `Sink::poll_ready` and `Sink::start_send`.
    ///
    /// This should return one of:
    ///
    /// * `Ok(None)` on successful and complete consumption of the item.
    /// * `Ok(Some(item))` if the sink is not ready (e.g. Poll::Pending),
    ///   returning the item for later retries.
    /// * `Err(e)` on any error occurring during a check for readiness or
    ///   consumption of the item.
    ///
    /// Note also that when returning `Ok(Some(item))`, this implementation or
    /// its delegate must eventually _wake_, via the passed `Context`
    /// accessible `Waker`, in order to continue.
    fn poll_send(self: Pin<&mut Self>, cx: &mut Context<'_>, item: Item)
        -> Result<Option<Item>, Self::SinkError>;

    /// Equivalent to `Sink::poll_flush`, with this default no-op
    /// implementation always returning `Poll::Ready(Ok(()))`.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::SinkError>>
    {
        Poll::Ready(Ok(()))
    }

    /// Equivalent to `Sink::poll_close`, with this default no-op
    /// implementation always returning `Poll::Ready(Ok(()))`.
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::SinkError>>
    {
        Poll::Ready(Ok(()))
    }
}

/// Wrapper type for TardySink, buffering a single item to implement the
/// standard `Sink`.
pub struct BufferedSink<Item, Ts>
    where Ts: TardySink<Item>
{
    ts: Ts,
    buf: Option<Item>
}

impl<Item, Ts> Unpin for BufferedSink<Item, Ts>
    where Ts: TardySink<Item> + Unpin
{}

impl<Item, Ts> BufferedSink<Item, Ts>
    where Ts: TardySink<Item>
{
    unsafe_pinned!(ts: Ts);
    unsafe_unpinned!(buf: Option<Item>);

    pub fn new(ts: Ts) -> Self {
        BufferedSink { ts, buf: None }
    }

    /// Consume self, returning the inner `TardySink`.
    pub fn into_inner(self) -> Ts {
        self.ts
    }

    fn poll_flush_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Ts::SinkError>>
    {
        if let Some(item) = self.as_mut().buf().take() {
            let tsp: Pin<&mut Ts> = self.as_mut().ts();
            match tsp.poll_send(cx, item) {
                Ok(None) => Poll::Ready(Ok(())),
                Ok(s @ Some(_)) => {
                    *self.as_mut().buf() = s;
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e))
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<Item, Ts> Sink<Item> for BufferedSink<Item, Ts>
    where Ts: TardySink<Item>
{
    type SinkError = Ts::SinkError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::SinkError>>
    {
        self.poll_flush_buf(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item)
        -> Result<(), Self::SinkError>
    {
        assert!(self.buf.is_none());
        *self.buf() = Some(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::SinkError>>
    {
        match self.as_mut().poll_flush_buf(cx) {
            Poll::Ready(Ok(_)) => {
                let tsp: Pin<&mut Ts> = self.as_mut().ts();
                tsp.poll_flush(cx)
            }
            res => res
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::SinkError>>
    {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(_)) => {
                let tsp: Pin<&mut Ts> = self.as_mut().ts();
                tsp.poll_close(cx)
            }
            res => res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::stream::StreamExt;

    struct TestSink {
        out: Vec<u8>,
        count: usize,
    }

    impl TestSink {
        fn new() -> TestSink {
            TestSink { out: Vec::with_capacity(2), count: 0 }
        }
    }

    impl TardySink<u8> for TestSink {
        type SinkError = ();

        fn poll_send(mut self: Pin<&mut Self>, cx: &mut Context<'_>, item: u8)
            -> Result<Option<u8>, Self::SinkError>
        {
            self.count += 1;

            // For testing, push back as Pending every other time
            if self.count % 2 == 0 {
                eprintln!("poll send back (count: {})", self.count);

                // Waking is currently needed. Test never completes without it.
                //
                // FIXME: Currently assuming this is an appropriate, TardySink
                // impl. requirement. Alternatively, it would need to be
                // included in BufferedSink, whenever it returns Pending.
                cx.waker().wake_by_ref();

                Ok(Some(item))
            } else {
                self.out.push(item);
                eprintln!("poll send pushed, output: {:?}", self.out);
                Ok(None)
            }
        }
    }

    #[test]
    fn forward_small() {
        let task = async {
            let stream = futures::stream::iter(vec![0u8, 1, 2, 3, 4]);
            let mut sink = BufferedSink::new(TestSink::new());
            stream
                .map(|i| Ok(i))
                .forward(&mut sink)
                .await
                .expect("forward");

            let ts = sink.into_inner();
            assert_eq!(vec![0u8, 1, 2, 3, 4], ts.out);

            let res: Result<(),()> = Ok(());
            res
        };
        block_on(task).expect("task success")
    }

}
