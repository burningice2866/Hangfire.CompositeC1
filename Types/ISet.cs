using System;

using Composite.Data;
using Composite.Data.Hierarchy;
using Composite.Data.Hierarchy.DataAncestorProviders;

namespace Hangfire.CompositeC1.Types
{
    [AutoUpdateble]
    [DataScope(DataScopeIdentifier.PublicName)]
    [DataAncestorProvider(typeof(NoAncestorDataAncestorProvider))]
    [ImmutableTypeId("51e6f50e-d797-46f9-b28e-002e9c824e45")]
    [Title("Set")]
    [KeyPropertyName("Id")]
    public interface ISet : IData
    {
        [StoreFieldType(PhysicalStoreFieldType.Guid)]
        [ImmutableFieldId("437e4527-7990-4f0e-b408-76dbafe0cd70")]
        [FunctionBasedNewInstanceDefaultFieldValue("<f:function name=\"Composite.Utils.Guid.NewGuid\" xmlns:f=\"http://www.composite.net/ns/function/1.0\" />")]
        Guid Id { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.String, 100)]
        [ImmutableFieldId("2c2eb3f0-4b0b-4236-972f-2430d167ab93")]
        string Key { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.Long)]
        [ImmutableFieldId("a606bc7d-be5e-4f70-8cc3-62ebf0379676")]
        long Score { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.String, 256)]
        [ImmutableFieldId("768ef921-3a2a-4ded-9d60-675da5fbcb7a")]
        string Value { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.DateTime)]
        [ImmutableFieldId("4752a7eb-b8d1-4e38-87e2-6790830ea0eb")]
        DateTime ExpireAt { get; set; }
    }
}
