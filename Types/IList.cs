using System;

using Composite.Data;
using Composite.Data.Hierarchy;
using Composite.Data.Hierarchy.DataAncestorProviders;

namespace Hangfire.CompositeC1.Types
{
    [AutoUpdateble]
    [DataScope(DataScopeIdentifier.PublicName)]
    [DataAncestorProvider(typeof(NoAncestorDataAncestorProvider))]
    [ImmutableTypeId("c741c3fc-749a-4962-bea4-e9ae9a5ecda0")]
    [Title("List")]
    [KeyPropertyName("Id")]
    public interface IList : IData
    {
        [StoreFieldType(PhysicalStoreFieldType.Guid)]
        [ImmutableFieldId("359dc0b0-5cfd-4fb9-89bd-df4d98c8bf11")]
        [FunctionBasedNewInstanceDefaultFieldValue("<f:function name=\"Composite.Utils.Guid.NewGuid\" xmlns:f=\"http://www.composite.net/ns/function/1.0\" />")]
        Guid Id { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.String, 100)]
        [ImmutableFieldId("584d2ce8-5467-4d89-83be-d82c21d4ed15")]
        string Key { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.LargeString)]
        [ImmutableFieldId("27ce95b6-b256-4a58-9057-f5316f6e2b9a")]
        string Value { get; set; }

        [StoreFieldType(PhysicalStoreFieldType.DateTime)]
        [ImmutableFieldId("0105304f-a8d1-4178-9e35-461838996a3b")]
        DateTime ExpireAt { get; set; }
    }
}
