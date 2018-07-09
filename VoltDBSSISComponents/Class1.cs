using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VoltDB.Data.Client;

namespace VoltDBSSISComponents
{
    [DtsPipelineComponent(
        DisplayName = "VoltDBSSISDestination",
        ComponentType = ComponentType.DestinationAdapter
        )]
    public class VoltDBSSISDestination : PipelineComponent
    {
        VoltConnection connection;

        // --------------DESIGN METHODS------------------------//
        public override void ProvideComponentProperties()
        {
            base.RemoveAllInputsOutputsAndCustomProperties();

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            //IDTSInputColumn100 col1 = input.InputColumnCollection.New();

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = input.ID;

            IDTSCustomProperty100 serverProp = ComponentMetaData.CustomPropertyCollection.New();
            serverProp.Name = "ServerAddress";
            serverProp.Description = "Hostname/IP address of VoltDB server";

            IDTSCustomProperty100 tableProp = ComponentMetaData.CustomPropertyCollection.New();
            tableProp.Name = "TableName";
            tableProp.Description = "Name of the VoltDB table to insert into";

            ComponentMetaData.ContactInfo = "ssomagani@voltdb.com";
            ComponentMetaData.Version = 121506;
        }

        // -------------------RUNTIME METHODS ------------------------//
        public override void AcquireConnections(object transaction)
        {
            base.AcquireConnections(transaction);

            Debug.WriteLine(ComponentMetaData.CustomPropertyCollection[0]);

            var hostname = ComponentMetaData.CustomPropertyCollection["ServerAddress"].Value;
            connection = VoltConnection.Create("hosts=" + hostname).Open();
        }

        public override void PerformUpgrade(int pipelineVersion)
        {
            //base.PerformUpgrade(pipelineVersion);
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            var tableName = ComponentMetaData.CustomPropertyCollection["TableName"].Value;
            IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);
            
            while(buffer.NextRow())
            {
                List<String> sqlStmtAruments = new List<string>();

                foreach(IDTSInputColumn100 col in input.InputColumnCollection)
                {
                    int colIndex = BufferManager.FindColumnByLineageID(input.Buffer, col.LineageID);
                    sqlStmtAruments.Add(buffer[colIndex].ToString());
                }

                String sqlStmt = "insert into " + tableName + " values (" + String.Join(", ", sqlStmtAruments) + ")";

                var Insert = connection.Procedures.Wrap<Null, string>("@AdHoc");

                Insert.Execute(sqlStmt);

                /* 

                 var genericMethod = typeof(ProcedureAccess).GetMethod("Wrap");
                 var typedMethod = genericMethod.MakeGenericMethod(
                     typeof(Null), 
                     typeof(Int32),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Int32)
                     );

                 object[] procNameParam = { "InsertOrder" };

                 var Insert = typedMethod.Invoke(connection.Procedures, procNameParam);
                 //var Insert = connection.Procedures.Wrap<Null, Int32, Double, Double, Double, Double, Double, Int32>("InsertOrder");

                 var genericProcMethod = typeof(ProcedureWrapper).GetMethod("Execute");
                 var typedProcMethod = genericProcMethod.MakeGenericMethod(
                     typeof(Null),
                     typeof(Int32),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Double),
                     typeof(Int32)
                     );

                 object[] procArgs = { productCode, shippingWeight, shippingLength, shippingWidth, shippingHeight, unitCost, perOrder };
                 typedProcMethod.Invoke(Insert, procArgs);*/

                //Insert.Execute(productCode, shippingWeight, shippingLength, shippingWidth, shippingHeight, unitCost, perOrder);
            }
        }
    }
}
