using System.Reflection.Metadata;
using System.Xml;
using System.Xml.Linq;
using Shared.garfiles;
using static Shared.MyLogger;

namespace importGeo.mssql.importGar
{
    public class ImportGarOne
    {
        public GarFiles GarFiles;
        public ImportGarOne(string GarRootPath)
        {
            this.GarFiles = new GarFiles(GarRootPath);
            this.GetXml1(this.GarFiles.GarMasters["AS_HOUSE_TYPES"].FileFullPath);
            //this.GetXml2(this.GarFiles.GarMasters["AS_HOUSE_TYPES"].FileFullPath);
            //Log.Information(this.GarFiles.PrintMasters());
        }

        public void GetXml1(string filePath)
        {
            Log.Information("Импорт из файла {filePath}", filePath);
            XDocument xDoc = XDocument.Load(filePath);
            foreach (XElement el in xDoc.Root.Elements())
            {

                int id = int.Parse(el.Attribute("ID").Value);
                string name = el.Attribute("NAME").Value;
                string? shortName = el.Attribute("SHORTNAME").Value;
                string? desc = el.Attribute("DESC").Value;
                bool isActive = bool.Parse(el.Attribute("ISACTIVE").Value);
                DateOnly updateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value);
                DateOnly startDate = DateOnly.Parse(el.Attribute("STARTDATE").Value);
                DateOnly endDate = DateOnly.Parse(el.Attribute("ENDDATE").Value);
                Log.Information("HouseTypeID:{id}, Name:{name}, ShortName:{shortName}, Desc:{desc}, IsActive:{isActive}, UpdateDate:{updateDate}, StartDate:{startDate}, EndDate:{endDate}",
                    id, name, shortName, desc, isActive, updateDate, startDate, endDate
                );
            }
        }

        public void GetXml2(string filePath)
        {
            Log.Information("Импорт из файла {filePath}", filePath);

            using (XmlReader reader = XmlReader.Create(filePath))
            {
                XDocument xDoc = XDocument.Load(reader);
                XElement? xRoot = xDoc.Element("HOUSETYPES");
                IEnumerable<XElement> houseTypes = xRoot.Elements("HOUSETYPE").Select(ht => ht);

                foreach (XElement el in houseTypes)
                {
                    string id = el.Attribute("ID").Value;
                    string name = el.Attribute("NAME").Value;
                    string shortName = el.Attribute("SHORTNAME").Value;
                    string desc = el.Attribute("DESC").Value;
                    string isActive = el.Attribute("ISACTIVE").Value;
                    string updateDate = el.Attribute("UPDATEDATE").Value;
                    string startDate = el.Attribute("STARTDATE").Value;
                    string endDate = el.Attribute("ENDDATE").Value;
                    Log.Information("HouseTypeID:{id}, Name:{name}, ShortName:{shortName}, Desc:{desc}, IsActive:{isActive}, UpdateDate:{updateDate}, StartDate:{startDate}, EndDate:{endDate}",
                        id, name, shortName, desc, isActive, updateDate, startDate, endDate
                    );
                }
            }
        }
    }
}
