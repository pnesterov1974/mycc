using System.Data;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class RunModel
    {
        public IEtlModel EtlModel { get; set; }
        public Source Source { get; set; }
        public Target Target { get; set; }
        public Sqls Sqls { get; set; }

        public RunModel(IEtlModel etlModel)
        {
            this.EtlModel = etlModel;

            this.Source = new Source(parentModel: this);
            this.Target = new Target(parentModel: this);
            this.Sqls = new Sqls(parentModel: this);
            this.Target.ReadIfObjectExists();
        }

        public void Run()
        {
            //this.Target.TouchObject(deleteIfExists: true);
            this.RunBulkInsert();
            //Log.Information(this.Sqls.GetMergeSql());
            //this.RunPages(20000);
            //this.RunBatches(20000);
            //this.RunInsertInto();
        }

        private void RunBulkInsert()
        {
            Log.Information("Загрузка {source} => {targer} используя BulkInsert", this.Source.Name, this.Target.FullName);
            if ((this.Source.ConnectionOk) && (this.Target.ConnectionOk))
            {
                using (var sourceConnection = new SqlConnection(this.Source.ConnectionString))
                {
                    sourceConnection.Open();
                    
                    using (var SourceCommand = new SqlCommand(this.Source.SelectSql, sourceConnection))
                    {
                        SqlDataReader sourceReader = SourceCommand.ExecuteReader();

                        using (var targetConnection = new SqlConnection(this.Target.ConnectionString))
                        {
                            targetConnection.Open();
                    
                            using (var bulkCopy = new SqlBulkCopy(targetConnection))
                            {
                                bulkCopy.DestinationTableName = this.Target.FullName;
                                bulkCopy.BulkCopyTimeout = 360;
                                //bulkCopy.ColumnMappings.Add("FirstName", "FirstName");
                                //bulkCopy.ColumnMappings.Add("LastName", "LastName");
                                //bulkCopy.ColumnMappings.Add("Email", "Email");
                                //bulkCopy.ColumnMappings.Add("Department", "Department");
                                try
                                {
                                    // Write from the source to the destination
                                    bulkCopy.WriteToServer(sourceReader);
                                    Console.WriteLine("Загрузка данных успешно завершена.");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Ошибка загрузка данных:\n {ex.Message}");
                                }
                            }
                        }
                    }
                }
            }
        }

        private void RunInsertInto()
        {
            Log.Information("Загрузка {source} => {targer} используя INSERT INTO", this.Source.Name, this.Target.FullName);
            this.Target.RunInsertInto();
        }

        private void RunPages(int PageSize = 10000)
        {
            Log.Information("Загрузка {source} => {targer} используя Пагинацию", this.Source.Name, this.Target.FullName);
            int currentPage = 0;
            int RecordsQueried = 0;
            do
            {
                currentPage += 1;
                Log.Information("Страница {currentPage}", currentPage);
                DataTable dt = this.Source.ReadPage(currentPage, PageSize, out RecordsQueried);
            }
            while (RecordsQueried > 0);
        }

        private void RunBatches(int batchSize = 100000)
        {
            Log.Information("Загрузка {source} => {targer} используя Batches через функцию ROW_NUMBER()", this.Source.Name, this.Target.FullName);
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
                DataTable dt = this.Source.ReadBatch(rnnFrom, rnnTo, out RecordsQueried);
            }
            while (RecordsQueried > 0);
        }
    }
}
