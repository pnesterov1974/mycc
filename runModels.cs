using System.Data;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;
using Shared;
using Shared.connections;

namespace Dataread.mssql
{
    public class RunModel
    {
        private IReadModel readModel;
        public IReadModel ReadModel 
        { 
            get
            {
                if (this.readModel != null)
                {
                    return this.readModel;
                }
                else
                {
                    throw new NullReferenceException("RunModel: etlModel не инициализирован");
                }
            }
        }

        private Source source;
        public Source Source
        {
            get
            {
                if (this.source != null)
                {
                    return this.source;
                }
                else
                {
                    throw new NullReferenceException("RunModel: Объект Elt.Model source не инициализирован");
                }
            }
        }

        private bool readModelIsValid = false;

        public RunModel(IReadModel readModel)
        {
            if (readModel == null)
            {
                throw new ArgumentNullException("readModelSource.sourceDbStru: в конструктор Source передан parentModel: null");
            }
            else
            {
                this.readModel = readModel;
                ValidateModel vm = new ValidateModel(TargetDb.mssql, this.readModel);
                this.readModelIsValid = vm.Validate();
            }
        }
    }
}
