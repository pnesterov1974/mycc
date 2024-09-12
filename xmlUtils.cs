using System.Xml;

namespace xmlUtils;

public static class XMLUtils
{
    public static string GetStringNotNullNodeValue(XmlElement xnode, string attrName)
    {
        string res = string.Empty;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = attr.Value;
        }
        catch
        { ; }
        return res;
    }

    public static string? GetStringNullNodeValue(XmlElement xnode, string attrName)
    {
        string? res = null;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = attr.Value;
        }
        catch
        { ; }
        return res;
    }

    public static int GetIntNotNullNodeValue(XmlElement xnode, string attrName)
    {
        int res = 0;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = int.Parse(attr.Value);
        }
        catch
        { ; }
        return res;
    }

    public static int? GetIntNullNodeValue(XmlElement xnode, string attrName)
    {
        int? res = null;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = int.Parse(attr.Value);
        }
        catch
        { ; }
        return res;
    }

    public static DateOnly GetDateOnlyNotNullNodeValue(XmlElement xnode, string attrName)
    {
        DateOnly res = new DateOnly();
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = DateOnly.Parse(attr.Value);
        }
        catch
        { ; }
        return res;
    }

    public static DateOnly? GetDateOnlyNullNodeValue(XmlElement xnode, string attrName)
    {
        DateOnly? res = null;
        try
        {
            XmlNode? attr = xnode.Attributes.GetNamedItem(attrName);
            res = DateOnly.Parse(attr.Value);
        }
        catch
        { ; }
        return res;
    }

    public static bool? GetBoolNullNodeValue(XmlElement xnode, string attrName)
    {
        try
        {
            XmlNode IsAttr = xnode.Attributes.GetNamedItem(attrName);
            string isAttr = IsAttr.Value;
            
            if (isAttr.ToLower().Equals("true"))
                {
                    return true;
                }
                else if (isAttr.ToLower().Equals("false"))
                {
                    return false;
                }
        }
        catch
        { ; }
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
