module mysql.inserter;

import std.array;
import std.meta;
import std.range;
import std.string;
import std.traits;
import std.typecons;

import mysql.connection;
import mysql.exception;
import mysql.protocol;
import mysql.type;

enum OnDuplicate : size_t
{
    Ignore,
    Error,
    Replace,
    Update,
    UpdateAll,
}

Inserter inserter(ref Connection connection)
{
    return Inserter(&connection);
}

Inserter inserter(Args...)(ref Connection connection, OnDuplicate action, string tableName, Args columns)
{
    auto insert = Inserter(&connection);
    insert.start(action, tableName, columns);
    return insert;
}

Inserter inserter(Args...)(ref Connection connection, string tableName, Args columns)
{
    auto insert = Inserter(&connection);
    insert.start(OnDuplicate.Error, tableName, columns);
    return insert;
}

private template isSomeStringOrSomeStringArray(T)
{
    enum isSomeStringOrSomeStringArray = isSomeString!(OriginalType!T) || (isArray!T && isSomeString!(ElementType!T));
}

struct Inserter
{
    //@disable this();
    //@disable this(this);

    this(Connection* connection)
    {
        conn_ = connection;
        pending_ = 0;
        flushes_ = 0;
    }

    ~this()
    {
        flush();
    }

    void start(Args...)(string tableName, Args fieldNames) if (Args.length && allSatisfy!(isSomeStringOrSomeStringArray, Args))
    {
        start(OnDuplicate.Error, tableName, fieldNames);
    }

    void start(Args...)(OnDuplicate action, string tableName, Args fieldNames) if (Args.length && allSatisfy!(isSomeStringOrSomeStringArray, Args))
    {
        auto fieldCount = fieldNames.length;

        foreach (size_t i, Arg; Args) {
            static if (isArray!Arg && !isSomeString!(OriginalType!Arg))
            {
                fieldCount = (fieldCount - 1) + fieldNames[i].length;
            }
        }

        fields_ = fieldCount;

        Appender!(char[]) app;

        final switch(action) with (OnDuplicate)
        {
        case Ignore:
            app.put("insert ignore into ");
            break;
        case Replace:
            app.put("replace into ");
            break;
        case UpdateAll:
            Appender!(char[]) dupapp;

            foreach(size_t i, Arg; Args)
            {
                static if (isSomeString!(OriginalType!Arg))
                {
                    dupapp.put('`');
                    dupapp.put(fieldNames[i]);
                    dupapp.put("`=values(`");
                    dupapp.put(fieldNames[i]);
                    dupapp.put("`)");
                }
                else
                {
                    auto columns = fieldNames[i];
                    foreach (j, name; columns)
                    {
                        dupapp.put('`');
                        dupapp.put(name);
                        dupapp.put("`=values(`");
                        dupapp.put(name);
                        dupapp.put("`)");
                        if (j + 1 != columns.length)
                            dupapp.put(',');
                    }
                }
                if (i + 1 != Args.length)
                    dupapp.put(',');
            }
            dupUpdate_ = dupapp.data;
            goto case Update;
        case Update:
        case Error:
            app.put("insert into ");
            break;
        }

        app.put(tableName);
        app.put('(');

        foreach (size_t i, Arg; Args)
        {
            static if (isSomeString!(OriginalType!Arg))
            {
                fieldsHash_ ~= hashOf(fieldNames[i]);
                fieldsNames_ ~= fieldNames[i];

                app.put('`');
                app.put(fieldNames[i]);
                app.put('`');
            }
            else
            {
                auto columns = fieldNames[i];
                foreach (j, name; columns)
                {

                    fieldsHash_ ~= hashOf(name);
                    fieldsNames_ ~= name;

                    app.put('`');
                    app.put(name);
                    app.put('`');
                    if (j + 1 != columns.length)
                        app.put(',');
                }
            }
            if (i + 1 != Args.length)
                app.put(',');
        }

        app.put(")values");
        start_ = app.data;
    }

    auto ref duplicateUpdate(string update)
    {
        dupUpdate_ = cast(char[])update;
        return this;
    }

    void rows(T)(ref const T[] param) if (!isValueType!T)
    {
        if (param.length < 1)
            return;

        foreach (ref p; param)
            row(p);
    }

    private auto tryAppendField(string member, string parentMembers = "", T)(ref const T param, ref size_t fieldHash, ref bool fieldFound)
    {
        static if (isReadableDataMember!(Unqual!T, member))
        {
            alias memberType = typeof(__traits(getMember, param, member));
            static if (isValueType!(memberType))
            {
                static if (getUDAs!(__traits(getMember, param, member), NameAttribute).length)
                {
                    enum nameHash = hashOf(parentMembers~getUDAs!(__traits(getMember, param, member), NameAttribute)[0].name);
                }
                else
                {
                    enum nameHash = hashOf(parentMembers~member);
                }
                if (nameHash == fieldHash || (parentMembers == "" && getUDAs!(T, UnCamelCaseAttribute).length && hashOf(member.unCamelCase) == fieldHash))
                {
                    appendValue(values_, __traits(getMember, param, member));
                    fieldFound = true;
                    return;
                }
            }
            else
            {
                foreach (subMember; __traits(allMembers, memberType))
                {
                    static if (parentMembers == "")
                    {
                        tryAppendField!(subMember, member~".")(__traits(getMember, param, member), fieldHash, fieldFound);
                    }
                    else
                    {
                        tryAppendField!(subMember, parentMembers~member~".")(__traits(getMember, param, member), fieldHash, fieldFound);
                    }

                    if (fieldFound)
                        return;
                }
            }
        }
    }

    void row (T) (ref const T param) if (!isValueType!T)
    {
        scope (failure) reset();

        if (start_.empty)
            throw new MySQLErrorException("Inserter must be initialized with a call to start()");

        if (!pending_)
            values_.put(cast(char[])start_);

        values_.put(pending_ ? ",(" : "(");
        ++pending_;

        bool fieldFound;
        foreach (i, ref fieldHash; fieldsHash_)
        {
            fieldFound = false;
            foreach (member; __traits(allMembers, T))
            {
                 tryAppendField!member(param, fieldHash, fieldFound);
                 if (fieldFound)
                     break;
            }
            if (!fieldFound)
                throw new MySQLErrorException(format("field '%s' was not found in struct => '%s' members", fieldsNames_.ptr[i], typeid(Unqual!T).name));

            if (i != fields_-1)
                values_.put(',');
        }
        values_.put(')');

        if (values_.data.length > (128 << 10)) // todo: make parameter
            flush();

        ++rows_;
    }

    void row(Values...)(Values values) if(allSatisfy!(isValueType, Values))
    {

        scope(failure) reset();

        if (start_.empty)
            throw new MySQLErrorException("Inserter must be initialized with a call to start()");

        auto valueCount = values.length;

        foreach (size_t i, Value; Values) {
            static if (isArray!Value && !isSomeString!(OriginalType!Value))
            {
                valueCount = (valueCount - 1) + values[i].length;
            }
        }

        if (valueCount != fields_)
            throw new MySQLErrorException(format("Wrong number of parameters for row. Got %d but expected %d.", valueCount, fields_));

        if (!pending_)
            values_.put(cast(char[])start_);

        values_.put(pending_ ? ",(" : "(");
        ++pending_;
        foreach (size_t i, Value; Values) {
            static if (isArray!Value && !isSomeString!(OriginalType!Value))
            {
                ValueAppender.appendValues(values_, values[i]);
            }
            else
            {
                ValueAppender.appendValue(values_, values[i]);
            }
            if (i != values.length-1)
                values_.put(',');
        }
        values_.put(')');

        if (values_.data.length > (128 << 10)) // todo: make parameter
            flush();

        ++rows_;
    }

    @property size_t rows() const
    {
        return rows_ != 0;
    }

    @property size_t pending() const
    {
        return pending_ != 0;
    }

    @property size_t flushes() const
    {
        return flushes_;
    }

    private void reset()
    {
        values_.clear;
        pending_ = 0;
    }

    void flush(string File = __FILE__, size_t Line = __LINE__)()
    {
        if (pending_)
        {
            if (dupUpdate_.length)
            {
                values_.put(cast(ubyte[])" on duplicate key update ");
                values_.put(cast(ubyte[])dupUpdate_);
            }

            auto sql = cast(char[])values_.data();
            reset();

            conn_.execute!(File, Line)(sql);
            ++flushes_;
        }
    }

private:
    char[] start_;
    char[] dupUpdate_;
    Appender!(char[]) values_;

    Connection* conn_;
    size_t pending_;
    size_t flushes_;
    size_t fields_;
    size_t rows_;
    string[] fieldsNames_;
    size_t[] fieldsHash_;
}

struct ValueAppender
{
    static void appendValues(Appender, T)(ref Appender appender, T values) if (isArray!T && !isSomeString!(OriginalType!T))
    {
        foreach (size_t i, value; values)
        {
            appendValue(appender, value);
            if (i != values.length-1)
                appender.put(',');
        }
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == typeof(null)))
    {
        appender.put("null");
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T))
    {
        if (value.isNull) {
            appendValue(appender, null);
        } else {
            appendValue(appender, value.get);
        }
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (isScalarType!T)
    {
        appender.put(cast(ubyte[])to!string(value));
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == SysTime))
    {
        value = value.toUTC;
    
        auto hour = value.hour;
        auto minute = value.minute;
        auto second = value.second;
        auto usec = value.fracSecs.total!"usecs";
    
        formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
        if (hour | minute | second | usec)
        {
            formattedWrite(appender, "%02d%02d%02d", hour, minute, second);
            if (usec)
                formattedWrite(appender, ".%06d", usec);
        }
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == DateTime))
    {
        auto hour = value.hour;
        auto minute = value.minute;
        auto second = value.second;
    
        if (hour | minute | second)
        {
            formattedWrite(appender, "%04d%02d%02d%02d%02d%02d", value.year, value.month, value.day, hour, minute, second);
        }
        else
        {
            formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
        }
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == TimeOfDay))
    {
        formattedWrite(appender, "%02d%02d%02d", value.hour, value.minute, value.second);
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == Date))
    {
        formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == Duration))
    {
        auto parts = value.split();
        if (parts.days)
        {
            appender.put('\'');
            formattedWrite(appender, "%d ", parts.days);
        }
        formattedWrite(appender, "%02d%02d%02d", parts.hours, parts.minutes, parts.seconds);
        if (parts.usecs)
            formattedWrite(appender, ".%06d ", parts.usecs);
        if (parts.days)
            appender.put('\'');
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == MySQLFragment))
    {
        appender.put(cast(char[])value.data);
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == MySQLRawString))
    {
        appender.put('\'');
        appender.put(cast(char[])value.data);
        appender.put('\'');
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == MySQLBinary))
    {
        appendValue(appender, value.data);
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == MySQLValue))
    {
        final switch(value.type) with (ColumnTypes)
        {
        case MYSQL_TYPE_NULL:
            appender.put("null");
            break;
        case MYSQL_TYPE_TINY:
            if (value.isSigned)
            {
                appendValue(appender, value.peek!byte);
            }
            else
            {
                appendValue(appender, value.peek!ubyte);
            }
            break;
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_SHORT:
            if (value.isSigned)
            {
                appendValue(appender, value.peek!short);
            }
            else
            {
                appendValue(appender, value.peek!ushort);
            }
            break;
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            if (value.isSigned)
            {
                appendValue(appender, value.peek!int);
            }
            else
            {
                appendValue(appender, value.peek!uint);
            }
            break;
        case MYSQL_TYPE_LONGLONG:
            if (value.isSigned)
            {
                appendValue(appender, value.peek!long);
            }
            else
            {
                appendValue(appender, value.peek!ulong);
            }
            break;
        case MYSQL_TYPE_DOUBLE:
            appendValue(appender, value.peek!double);
            break;
        case MYSQL_TYPE_FLOAT:
            appendValue(appender, value.peek!float);
            break;
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
            appendValue(appender, value.peek!(char[]));
            break;
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
            appendValue(appender, value.peek!(ubyte[]));
            break;
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
            appendValue(appender, value.peek!Duration);
            break;
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            appendValue(appender, value.peek!SysTime);
            break;
        }
    }
    
    static void appendValue(Appender, T)(ref Appender appender, T value) if (isArray!T && (is(Unqual!(typeof(T.init[0])) == ubyte) || is(Unqual!(typeof(T.init[0])) == char)))
    {
        appender.put('\'');
        auto ptr = value.ptr;
        auto end = value.ptr + value.length;
        while (ptr != end)
        {
            switch(*ptr)
            {
            case '\\':
            case '\'':
                appender.put('\\');
                goto default;
            default:
                appender.put(*ptr++);
            }
        }
        appender.put('\'');
    }
}
