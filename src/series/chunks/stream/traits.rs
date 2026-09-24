use num_traits::PrimInt;
use std::io;

/// Read bits from an underlying byte stream.
pub trait BitRead {
    /// Read a single bit from the underlying stream.
    fn read_bit(&mut self) -> io::Result<bool>;

    /// Read `num` bits from the underlying stream.
    fn read_bits(&mut self, num: u32) -> io::Result<u64>;
}

/// Write bits to an underlying byte stream.
pub trait BitWrite {
    /// Writes a single bit to the stream.
    fn write_bit(&mut self, bit: bool) -> io::Result<()>;

    /// Writes an unsigned value to the stream using the given number of bits.
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt;
}
