using System.Xml;
using static Shared.MyLogger;

namespace xmlUtils2;

public static class XMLUtils
{
    public static string GetStringNotNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        string res = string.Empty;
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = attr.Value;
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}, атрибут {attrn}", ObjectName, KeyValue, attrName);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static string? GetStringNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        string? res = null;
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = attr.Value;
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}, атрибут {attrn}", ObjectName, KeyValue, attrName);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static int GetIntNotNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        int res = 0;
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = int.Parse(attr.Value);
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}", ObjectName, KeyValue);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static int? GetIntNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        int? res = null;
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = int.Parse(attr.Value);
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}", ObjectName, KeyValue);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static DateOnly GetDateOnlyNotNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        DateOnly res = new DateOnly();
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = DateOnly.Parse(attr.Value);
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}", ObjectName, KeyValue);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static DateOnly? GetDateOnlyNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        DateOnly? res = null;
        bool isOk = false;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = DateOnly.Parse(attr.Value);
            isOk = true;
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}", ObjectName, KeyValue);
        }
        if (!isOk)
            ErrorCount += 1;
        return res;
    }

    public static bool? GetBoolNullNodeValue(XmlElement xnode, string attrName, string ObjectName, string KeyValue, ref int ErrorCount)
    {
        bool isOk = false;
        try
        {
            XmlNode IsAttr = xnode.Attributes.GetNamedItem(attrName);
            string isAttr = IsAttr.Value;

            if (isAttr.ToLower().Equals("true"))
            {
                isOk = true;
                return true;
            }
            else if (isAttr.ToLower().Equals("false"))
            {
                isOk = true;
                return false;
            }
        }
        catch
        {
            Log.Error("Ошибка в исходных данных: \n");
            Log.Error("Объект {object}, ключ строки {key}", ObjectName, KeyValue);
            ErrorCount += 1;
        }   
        return null;
    }
}

// TODO bool

// public static T GetNodeValue<T>(XmlElement xnode, string attrName)
// {
//     XmlNode attr = xnode.Attributes.GetNamedItem(attrName);
//     T res = attr.Value as T;
//     return Null; 
// }
