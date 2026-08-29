pub(crate) fn jittered_exponential_delay_ms(
    base_ms: u64,
    cap_ms: u64,
    failures: u8,
    scope: &str,
    entropy: u64,
) -> u64 {
    if base_ms == 0 || cap_ms < base_ms || failures == 0 {
        return 0;
    }
    let exponent = u32::from(failures.saturating_sub(1).min(15));
    let upper = base_ms.saturating_mul(1_u64 << exponent).min(cap_ms);
    let lower = (upper / 2).max(1);
    let span = upper.saturating_sub(lower).saturating_add(1);
    lower.saturating_add(jitter_hash(scope, failures, entropy) % span)
}

fn jitter_hash(scope: &str, failures: u8, entropy: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in scope
        .bytes()
        .chain(std::process::id().to_le_bytes())
        .chain([failures])
        .chain(entropy.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_is_bounded_exponential_and_scope_jittered() {
        for failures in 1..=12 {
            let upper = 250_u64
                .saturating_mul(1_u64 << u32::from((failures - 1).min(15)))
                .min(30_000);
            let delay = jittered_exponential_delay_ms(250, 30_000, failures, "account-a", 99);
            assert!((upper / 2).max(1) <= delay);
            assert!(delay <= upper);
        }
        assert_ne!(
            jittered_exponential_delay_ms(250, 30_000, 4, "account-a", 99),
            jittered_exponential_delay_ms(250, 30_000, 4, "account-b", 99)
        );
    }

    #[test]
    fn invalid_policy_never_creates_a_retry_delay() {
        assert_eq!(jittered_exponential_delay_ms(0, 30_000, 1, "a", 1), 0);
        assert_eq!(jittered_exponential_delay_ms(250, 100, 1, "a", 1), 0);
        assert_eq!(jittered_exponential_delay_ms(250, 30_000, 0, "a", 1), 0);
    }
}
