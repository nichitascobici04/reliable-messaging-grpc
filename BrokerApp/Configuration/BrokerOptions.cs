namespace BrokerApp.Configuration;

public sealed class BrokerOptions
{
    public const string SectionName = "Broker";

    public int Port { get; set; } = 5000;

    // How long the broker waits for an ACK before redelivering.
    public int AckTimeoutSeconds { get; set; } = 5;

    // Retries AFTER the first delivery (same meaning as MAX_RETRIES in Part 1).
    public int MaxRetries { get; set; } = 3;

    public int RetryIntervalMs { get; set; } = 1000;

    // A disconnected subscriber session is kept this long so that
    // a reconnect with the same subscriber_id can resume it.
    public int SessionExpiryMinutes { get; set; } = 10;

    public int MaxAttempts => 1 + MaxRetries;

    public FaultInjectionOptions FaultInjection { get; set; } = new();
}

public sealed class FaultInjectionOptions
{
    public bool Enabled { get; set; } = true;

    // Any published content containing this keyword triggers
    // the "response lost after commit" fault (first attempt only).
    public string LoseResponseKeyword { get; set; } = "#lose-response";

    // 0.0 - 1.0: random chance of losing a response (chaos mode).
    public double LostResponseProbability { get; set; } = 0.0;

    public LostResponseMode Mode { get; set; } = LostResponseMode.Delay;

    public int DelayMs { get; set; } = 5000;
}

public enum LostResponseMode
{
    // Hold the response longer than the client's deadline => DEADLINE_EXCEEDED.
    Delay,

    // Drop the call after commit => UNAVAILABLE.
    Abort
}