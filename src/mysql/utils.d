module mysql.utils;

import std.array;
import std.utf : decode, UseReplacementDchar;
import std.ascii;

struct Utils
{
    static auto copyUpToNext(ref Appender!(char[]) app, ref const(char)[] sql)
    {
        size_t offset;
        dchar quote = '\0';

        while (offset < sql.length)
        {
            auto ch = decode!(UseReplacementDchar.no)(sql, offset);
            switch (ch)
            {
            case '?':
                if (!quote)
                {
                    app.put(sql[0..offset - 1]);
                    sql = sql[offset..$];
                    return true;
                }
                else
                {
                    goto default;
                }
            case '\'':
            case '\"':
            case '`':
                if (quote == ch)
                {
                    quote = '\0';
                }
                else if (!quote)
                {
                    quote = ch;
                }
                goto default;
            case '\\':
                if (quote && (offset < sql.length))
                    decode!(UseReplacementDchar.no)(sql, offset);
                goto default;
            default:
                break;
            }
        }
        app.put(sql[0..offset]);
        sql = sql[offset..$];

        return false;
    }

    static bool appendNextValue(T)(ref Appender!(char[]) app, ref const(char)[] sql, ref size_t indexArg, const(void)* arg)
    {
        static if (isArray!T && !isSomeString!(OriginalType!T))
        {
            foreach (i, ref v; *cast(T*)arg)
            {
                if (copyUpToNext(app, sql))
                {
                    appendValue(app, v);
                    ++indexArg;
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            if (copyUpToNext(app, sql))
            {
                appendValue(app, *cast(T*)arg);
                ++indexArg;
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    static string unCamelCase(string x)
    {
        assert(x.length <= 64);

        enum CharClass
        {
            LowerCase,
            UpperCase,
            Underscore,
            Digit,
        }

        CharClass classify(char ch) @nogc @safe pure nothrow
        {
            switch (ch) with (CharClass)
            {
            case 'A':..case 'Z':
                return UpperCase;
            case 'a':..case 'z':
                return LowerCase;
            case '0':..case '9':
                return Digit;
            case '_':
                return Underscore;
            default:
                assert(false, "only supports identifier-type strings");
            }
        }

        if (x.length > 0)
        {
            char[128] buffer;
            size_t length;

            auto pcls = classify(x.ptr[0]);
            foreach (i; 0..x.length) with (CharClass)
            {
                auto ch = x.ptr[i];
                auto cls = classify(ch);

                final switch (cls)
                {
                case Underscore:
                    buffer[length++] = '_';
                    break;
                case LowerCase:
                    buffer[length++] = ch;
                    break;
                case UpperCase:
                    if ((pcls != UpperCase) && (pcls != Underscore))
                        buffer[length++] = '_';
                    buffer[length++] = std.ascii.toLower(ch);
                    break;
                case Digit:
                    if (pcls != Digit)
                        buffer[length++] = '_';
                    buffer[length++] = ch;
                    break;
                }
                pcls = cls;

                if (length == buffer.length)
                    break;
            }
            return buffer[0..length].idup;
        }
        return x;
    }
}