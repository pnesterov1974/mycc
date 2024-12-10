namespace Datareader.pgsql.KladrJustImportedModels;
public class KladrJustImportedCatalog
{
    public List<IReadModel> ReadModels = new List<IReadModel>
    {
        new SocrBaseJustImportedModel(),
        new AltNamesJustImportedModel(),
        new KladrJustImportedModel(),
        new StreetJustImportedModel(),
        new DomaJustImportedModel(),
    };
}
