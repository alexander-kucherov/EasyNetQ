using System.Runtime.Serialization;
namespace EasyNetQ;

[Serializable]
public class EasyNetQException : Exception
{
    /// <inheritdoc />
    public EasyNetQException() { }

    /// <inheritdoc />
    public EasyNetQException(string? message) : base(message) { }

    /// <inheritdoc />
    public EasyNetQException(string format, params object?[] args) : base(string.Format(format, args)) { }

    /// <inheritdoc />
    public EasyNetQException(string? message, Exception? inner) : base(message, inner) { }

#pragma warning disable SYSLIB0051
    /// <inheritdoc />
    protected EasyNetQException(SerializationInfo info, StreamingContext context) : base(info, context) { }
#pragma warning restore SYSLIB0051
}

[Serializable]
public class EasyNetQResponderException : EasyNetQException
{
    /// <inheritdoc />
    public EasyNetQResponderException() { }

    /// <inheritdoc />
    public EasyNetQResponderException(string? message) : base(message) { }

    /// <inheritdoc />
    public EasyNetQResponderException(string format, params object?[] args) : base(format, args) { }

    /// <inheritdoc />
    public EasyNetQResponderException(string? message, Exception? inner) : base(message, inner) { }

#pragma warning disable SYSLIB0051
    /// <inheritdoc />
    protected EasyNetQResponderException(SerializationInfo info, StreamingContext context) : base(info, context) { }
#pragma warning restore SYSLIB0051
}
