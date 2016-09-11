using System;

namespace Orleans.Serialization
{
    using System.Collections.Concurrent;
    using System.Reflection;

    public class IlBasedSerializers
    {
        private readonly ConcurrentDictionary<Type, SerializationManager.SerializerMethods> serializers =
            new ConcurrentDictionary<Type, SerializationManager.SerializerMethods>();

        private readonly IlBasedSerializerBuilder builder = new IlBasedSerializerBuilder();

        private readonly SerializationManager.SerializerMethods genericSerializer;

        public IlBasedSerializers()
        {
            this.genericSerializer = this.CreateGenericSerializer();
        }

        private SerializationManager.SerializerMethods CreateGenericSerializer()
        {
            SerializationManager.DeepCopier copier = original =>
            {
                if (original == null) return null;
                return this.GetAndRegister(original.GetType()).DeepCopy(original);
            };

            SerializationManager.Serializer serializer = (raw, stream, expected) =>
            {
                if (raw == null)
                {
                    stream.WriteNull();
                    return;
                }

                this.GetAndRegister(raw.GetType()).Serialize(raw, stream, expected);
            };

            SerializationManager.Deserializer deserializer = (expected, stream) => this.GetAndRegister(expected).Deserialize(expected, stream);

            return new SerializationManager.SerializerMethods(copier, serializer, deserializer);
        }

        private SerializationManager.SerializerMethods GetAndRegister(Type type)
        {
            var methods = this.Get(type);
            SerializationManager.Register(type, methods.DeepCopy, methods.Serialize, methods.Deserialize, forceOverride: true);
            return methods;
        }

        public SerializationManager.SerializerMethods Get(Type type)
        {
            if (type.IsGenericTypeDefinition) return this.genericSerializer;
            return this.serializers.GetOrAdd(type, t => this.builder.GenerateSerializer(t.GetTypeInfo()));
        }
    }
}
