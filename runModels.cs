using System.Data;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class RunModel
    {
        public IEtlModel EtlModel { get; set; }
        private Source source { get; set; }
        private Target target { get; set; }
        public Sqls Sqls { get; set; }

        public RunModel(IEtlModel etlModel)
        {
            this.EtlModel = etlModel;

            this.source = new Source(
                sourceName: this.EtlModel.SourceName,
                selectSql: this.EtlModel.SourceSql,
                keyFields: this.EtlModel.SourceKeyFilelds,
                parentModel: this
                );
            this.target = new Target(
                targetName: this.EtlModel.TargetName,
                targetSchemaName: this.EtlModel.TargetSchemaName,
                targetTableName: this.EtlModel.TargetTableName,
                keyFields: this.EtlModel.SourceKeyFilelds,
                parentModel: this
                );
            this.Sqls = new Sqls(src: this.source, trg: this.target);
            this.target.ReadIfObjectExists();
        }

        public void Run()
        {
            this.target.TouchObject(deleteIfExists: true);
            this.RunBatches(10000);
        }

        public void RunInsertInto()
        {
            this.target.RunInsertInto();
        }

        public void RunBatches(int batchSize=100000)
        {
            int rnnInitialFrom = 0;
            int rnnInitialTo = batchSize;
            int rnnFrom = 0;
            int rnnTo = 0;
            int iterCount = 0;
            int RecordsQueried = 0;
            do
            {
                iterCount += 1;
                if (iterCount == 1)
                {
                    rnnFrom = rnnInitialFrom + 1;
                    rnnTo = rnnInitialTo;
                }
                else
                {
                    rnnFrom = rnnTo + 1;
                    rnnTo = rnnTo + batchSize;
                }
                Log.Information("Итерация {iterCount} Диапазон по счетчику Rnn от {from} до {to}", iterCount, rnnFrom, rnnTo);
                DataTable dt = this.source.ReadBatch(rnnFrom, rnnTo, out RecordsQueried);
            }
            while (RecordsQueried > 0);
        }
    }
}
