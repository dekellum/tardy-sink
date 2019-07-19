#![warn(rust_2018_idioms)]
#![feature(async_await)]

use std::pin::Pin;
use std::task::{Context, Poll};
use futures::sink::Sink;
use pin_utils::{unsafe_pinned, unsafe_unpinned};

/// A sink which doesn't know it's readiness until it tries to consume an
/// Item. An implementation of this trait may be adapted to the standard
/// futures `Sink` by calling [`TardySinkExt::into_sink`].
pub trait TardySink<Item>
{
    type Error;

    /// Attempt to consume an `Item`, as a logical combination of
    /// `Sink::poll_ready` and `Sink::start_send`.
    ///
    /// This should return one of:
    ///
    /// * `Ok(None)` on successful and complete consumption of the item.
    /// * `Ok(Some(item))` if the sink is not ready (e.g. Poll::Pending),
    ///   returning the same (possibly modified) item, or a different `Item`,
    ///   for subsequent retry.
    /// * `Err(e)` on any error occurring during a check for readiness or
    ///   consumption of the item.
    ///
    /// Note also that when returning `Ok(Some(item))`, this implementation or
    /// its delegate must eventually _wake_, via the passed `Context`
    /// accessible `Waker`, in order to continue.
    fn poll_send(self: Pin<&mut Self>, cx: &mut Context<'_>, item: Item)
        -> Result<Option<Item>, Self::Error>;

    /// Equivalent to `Sink::poll_flush`, with this default no-op
    /// implementation always returning `Poll::Ready(Ok(()))`.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        Poll::Ready(Ok(()))
    }

    /// Equivalent to `Sink::poll_close`, with this default no-op
    /// implementation always returning `Poll::Ready(Ok(()))`.
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        Poll::Ready(Ok(()))
    }
}

/// An extension trait for `TardySink`s offering convenience methods.
pub trait TardySinkExt<Item>: TardySink<Item> {
    /// Consume self and return a `Sink` adapter
    fn into_sink(self) -> OneBuffer<Self, Item>
        where Self: Sized
    {
        OneBuffer::new(self)
    }
}

impl<T, Item> TardySinkExt<Item> for T
    where T: TardySink<Item>
{}

/// Wrapper type for TardySink, buffering a single item to implement the
/// standard `Sink`. This is normally constructed via [`TardySinkExt::into_sink`].
pub struct OneBuffer<Ts, Item>
    where Ts: TardySink<Item>
{
    ts: Ts,
    buf: Option<Item>
}

impl<Ts, Item> Unpin for OneBuffer<Ts, Item>
    where Ts: TardySink<Item> + Unpin
{}

impl<Ts, Item> OneBuffer<Ts, Item>
    where Ts: TardySink<Item>
{
    unsafe_pinned!(ts: Ts);
    unsafe_unpinned!(buf: Option<Item>);

    pub fn new(ts: Ts) -> Self {
        OneBuffer { ts, buf: None }
    }

    /// Consume self, returning the inner `TardySink`.
    pub fn into_inner(self) -> Ts {
        self.ts
    }

    fn poll_flush_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Ts::Error>>
    {
        if let Some(item) = self.as_mut().buf().take() {
            match self.as_mut().ts().poll_send(cx, item) {
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

impl<Ts, Item> Sink<Item> for OneBuffer<Ts, Item>
    where Ts: TardySink<Item>
{
    type Error = Ts::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        self.poll_flush_buf(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item)
        -> Result<(), Self::Error>
    {
        assert!(self.buf.is_none());
        *self.buf() = Some(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        match self.as_mut().poll_flush_buf(cx) {
            Poll::Ready(Ok(_)) => {
                self.as_mut().ts().poll_flush(cx)
            }
            res => res
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(_)) => {
                self.as_mut().ts().poll_close(cx)
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
        type Error = ();

        fn poll_send(mut self: Pin<&mut Self>, cx: &mut Context<'_>, item: u8)
            -> Result<Option<u8>, Self::Error>
        {
            self.count += 1;

            // For testing, push back as Pending only periodically.
            if self.count % 3 == 0 || self.count % 4 == 0 {
                eprintln!("poll send back (count: {})", self.count);

                // Waking is currently needed. Test never completes without it.
                //
                // FIXME: Currently assuming this is an appropriate, TardySink
                // impl. requirement. Alternatively, it would need to be
                // included in OneBuffer, whenever it returns Pending.
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
            let vals: Vec<u8> = (0u8..100u8).collect();
            let stream = futures::stream::iter(vals.clone());
            let mut sink = TestSink::new().into_sink();
            stream
                .map(|i| Ok(i))
                .forward(&mut sink)
                .await
                .expect("forward");

            let ts = sink.into_inner();
            assert_eq!(vals, ts.out);

            let res: Result<(),()> = Ok(());
            res
        };
        block_on(task).expect("task success")
    }

}
