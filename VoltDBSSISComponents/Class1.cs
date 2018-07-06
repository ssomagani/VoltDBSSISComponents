using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoltDBSSISComponents
{
    [DtsPipelineComponent(
        DisplayName = "VoltDBSSISDestination",
        ComponentType = ComponentType.DestinationAdapter
        )]
    public class VoltDBSSISDestination : PipelineComponent
    {
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

            IDTSCustomProperty100 property = ComponentMetaData.CustomPropertyCollection.New();
            property.Name = "ServerAddress";
            property.Description = "Hostname/IP address of VoltDB server";

            ComponentMetaData.ContactInfo = "ssomagani@voltdb.com";
            ComponentMetaData.Version = 1017;
        }

        // -------------------RUNTIME METHODS ------------------------//
        public override void AcquireConnections(object transaction)
        {
            base.AcquireConnections(transaction);
            Console.WriteLine("Acquiring connections");
        }

        public override void PerformUpgrade(int pipelineVersion)
        {
            //base.PerformUpgrade(pipelineVersion);
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            base.ProcessInput(inputID, buffer);
            Console.WriteLine("Processing input " + inputID);
        }
    }
}
