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
            if (!this.Source.ConnectionOk || !this.Target.ConnectionOk)
            {
                ; // throw exception stop work
            }
            else
            {
                this.Target.TouchObject(deleteIfExists: true);
                //this.RunBulkInsert();
                //Log.Information(this.Sqls.GetMergeSql());
                this.RunPages(20000);
                //this.RunBatches(20000);
                //this.RunInsertInto();
            }
        }

        private void RunInsertInto()
        {
            string insertIntoSql = this.Sqls.GetInsertIntoSql();
            Log.Information("Заполнение таблицы {t}", this.Target.FullName);
            Log.Debug("insertIntoSql:\n{insertIntoSql}", insertIntoSql);
            using (SqlConnection conn = new SqlConnection(this.Target.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(insertIntoSql, conn))
                {
                    try
                    {
                        conn.Open();
                        int result = cmd.ExecuteNonQuery();
                        Log.Information("Объект {t} успешно заполнен, вставлено {r} записей", this.Target.FullName, result);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ощибка заполнения объекта {t}\n", this.Target.FullName);
                        Log.Debug("{errmes}", ex.Message);
                    }
                }
            }
        }

        private void RunPages(int PageSize = 10000)
        {
            Log.Information("Загрузка {source} => {targer} используя Пагинацию", this.Source.Name, this.Target.FullName);
            int currentPage = 0;
            int currentRecordsQueried = 0;
            int totalRecordsQueried = 0;
            int totalRecordsSaved = 0;
            do
            {
                currentPage += 1;
                Log.Information("Страница {currentPage}", currentPage);
                DataTable dt = this.Source.ReadPage(currentPage, PageSize, out currentRecordsQueried);
                totalRecordsQueried += currentRecordsQueried;
                totalRecordsSaved += this.Target.SaveBatch(dt);
                Log.Information("Загружено записей {recs} в {target}", totalRecordsSaved, this.Target.FullName);
            }
            while (currentRecordsQueried > 0);
            Log.Information("Всего считано {pages} страниц", currentPage);
            Log.Information("Загружено из источника {recs} в записей", totalRecordsQueried);
            Log.Information("Записано в {target} {records} записей", this.Target.FullName, totalRecordsSaved);
        }

        private void RunBatches(int batchSize = 100000)
        {
            int rnnInitialFrom = 0;
            int rnnInitialTo = batchSize;
            int rnnFrom = 0;
            int rnnTo = 0;
            int iterCount = 0;
            int RecordsQueried = 0;
            int RecordsSaved = 0;
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
                RecordsSaved += this.Target.SaveBatch(dt);
                Log.Information("Загружено записей {recs} в {target}", RecordsSaved, this.Target.FullName);
            }
            while (RecordsQueried > 0);
        }
    }
}
