# Chunk encoding latency

Median of 300 iterations after 50 warmup runs. Times are µs for the whole 1000-sample operation.

`encode_bulk` = `set_data`; `encode_append` = per-sample `add_sample`; `decode_iter` = full scan; `get_range` = full-range materialization; `scan_10%` = a 10% window centered mid-chunk.

## bursty / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 20.4 | 21.8 | 12.0 | 12.9 | 6.17 | 6241 |
| gorilla | 8.4 | 9.2 | 10.2 | 10.7 | 5.08 | 6595 |

## bursty_q2 / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 12.0 | 13.2 | 11.2 | 12.5 | 4.58 | 1934 |
| gorilla | 8.8 | 9.1 | 10.2 | 10.5 | 4.75 | 4796 |

## constant / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 4.8 | 4.8 | 6.2 | 7.3 | 3.17 | 516 |
| gorilla | 4.5 | 4.9 | 6.8 | 6.9 | 3.25 | 266 |

## counter / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 24.8 | 27.5 | 11.0 | 12.3 | 5.83 | 2378 |
| gorilla | 10.9 | 11.9 | 11.4 | 11.7 | 5.25 | 1759 |

## discrete / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 23.3 | 24.3 | 10.4 | 11.5 | 4.96 | 2207 |
| gorilla | 8.8 | 9.8 | 10.1 | 10.8 | 4.54 | 1343 |

## drift / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 20.2 | 22.0 | 12.0 | 12.9 | 6.38 | 6006 |
| gorilla | 8.7 | 9.5 | 10.4 | 10.5 | 4.92 | 5984 |

## drift_q2 / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 11.3 | 12.5 | 11.3 | 12.2 | 5.04 | 1491 |
| gorilla | 9.0 | 10.1 | 10.9 | 11.4 | 5.29 | 4255 |

## noisy / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 20.5 | 22.8 | 11.4 | 12.2 | 11.38 | 7411 |
| gorilla | 9.0 | 9.8 | 10.5 | 11.1 | 5.38 | 7194 |

## noisy_q2 / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 17.4 | 18.6 | 13.5 | 15.1 | 5.88 | 3669 |
| gorilla | 9.1 | 9.9 | 10.3 | 10.8 | 5.62 | 7169 |

## periodic / regular

| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |
|---|---:|---:|---:|---:|---:|---:|
| chimp | 20.1 | 22.0 | 11.6 | 12.5 | 5.25 | 7039 |
| gorilla | 8.2 | 8.9 | 10.5 | 10.1 | 4.96 | 7247 |

