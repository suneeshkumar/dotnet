namespace Orleans.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;

    using Orleans.Runtime;

    using Sigil;

    internal class IlBasedSerializerBuilder
    {
        /// <summary>
        /// A reference to a method which returns an unformatted object.
        /// </summary>
        private readonly MethodInfo getUninitializedObjectMethodInfo;

        /// <summary>
        /// A reference to <see cref="Type.GetTypeFromHandle"/>.
        /// </summary>
        private readonly MethodInfo getTypeFromHandleMethodInfo;

        /// <summary>
        /// A reference to the <see cref="SerializationContext.Current"/> getter method.
        /// </summary>
        private readonly MethodInfo getCurrentSerializationContext;

        /// <summary>
        /// A reference to the <see cref="SerializationContext.RecordObject(object, object)"/> method.
        /// </summary>
        private readonly MethodInfo recordObjectWhileCopyingMethodInfo;

        /// <summary>
        /// A reference to <see cref="SerializationManager.DeepCopyInner"/>
        /// </summary>
        private readonly MethodInfo deepCopyInnerMethodInfo;

        /// <summary>
        /// A reference to the <see cref="SerializationManager.SerializeInner(object, BinaryTokenStreamWriter, Type)"/> method.
        /// </summary>
        private readonly MethodInfo serializeInnerMethodInfo;

        /// <summary>
        /// A reference to the <see cref="SerializationManager.DeserializeInner(Type, BinaryTokenStreamReader)"/> method.
        /// </summary>
        private readonly MethodInfo deserializeInnerMethodInfo;

        private readonly MethodInfo recordObjectWhileDeserializingMethodInfo;

        private readonly MethodInfo getCurrentDeserializationContext;

        public IlBasedSerializerBuilder()
        {
#if NETSTANDARD
            this.getUninitializedObjectMethodInfo = TypeUtils.Method(() => SerializationManager.GetUninitializedObjectWithFormatterServices(typeof(int)));
#else
            this.getUninitializedObjectMethodInfo = TypeUtils.Method(() => FormatterServices.GetUninitializedObject(typeof(int)));
#endif
            this.getTypeFromHandleMethodInfo = TypeUtils.Method(() => Type.GetTypeFromHandle(typeof(int).TypeHandle));

            this.deepCopyInnerMethodInfo = TypeUtils.Method(() => SerializationManager.DeepCopyInner(typeof(int)));
            this.serializeInnerMethodInfo =
                TypeUtils.Method(() => SerializationManager.SerializeInner(default(object), default(BinaryTokenStreamWriter), default(Type)));
            this.deserializeInnerMethodInfo = TypeUtils.Method(() => SerializationManager.DeserializeInner(default(Type), default(BinaryTokenStreamReader)));

            this.getCurrentSerializationContext = TypeUtils.Property((object _) => SerializationContext.Current).GetMethod;
            this.recordObjectWhileCopyingMethodInfo = TypeUtils.Method((SerializationContext ctx) => ctx.RecordObject(default(object), default(object)));

            this.getCurrentDeserializationContext = TypeUtils.Property((object _) => DeserializationContext.Current).GetMethod;
            this.recordObjectWhileDeserializingMethodInfo = TypeUtils.Method((DeserializationContext ctx) => ctx.RecordObject(default(object)));
        }
        
        public SerializationManager.SerializerMethods GenerateSerializer(TypeInfo type)
        {
            try
            {
                var fields = this.GetFields(type);
                var copier = this.EmitCopier(type, fields);
                var serializer = this.EmitSerializer(type, fields);
                var deserializer = this.EmitDeserializer(type, fields);
                return new SerializationManager.SerializerMethods(copier, serializer, deserializer);
            }
            catch (Exception exception)
            {
                throw new IlBasedSerializerException($"Serializer generation failed for type {type}", exception);
            }
        }

        private SerializationManager.DeepCopier EmitCopier(TypeInfo type, List<FieldInfo> fields)
        {
            var emitter = Emit<SerializationManager.DeepCopier>.NewDynamicMethod(type, type.Name + "DeepCopier");

            // Declare local variables.
            var result = emitter.DeclareLocal(type);
            var typedInput = emitter.DeclareLocal(type);

            // Set the typed input variable from the method parameter.
            emitter.LoadArgument(0);
            emitter.CastOrUnbox(type);
            emitter.StoreLocal(typedInput);

            // Construct the result.
            this.CreateInstance(emitter, type, result);

            // Record the object.
            emitter.Call(this.getCurrentSerializationContext);
            emitter.LoadArgument(0); // Load 'original' parameter.
            emitter.LoadLocal(result); // Load 'result' local.
            if (type.IsValueType) emitter.Box(type);
            emitter.Call(this.recordObjectWhileCopyingMethodInfo);

            // Copy each field.
            foreach (var field in fields)
            {
                emitter.LoadLocalAsReference(type, result);
                emitter.LoadLocal(typedInput);
                emitter.LoadField(field);

                // Deep-copy the field if needed, otherwise just leave it as-is.
                if (!field.FieldType.IsOrleansShallowCopyable())
                {
                    if (field.FieldType.IsValueType) emitter.Box(field.FieldType);
                    emitter.Call(this.deepCopyInnerMethodInfo);
                    emitter.CastOrUnbox(field.FieldType);
                }

                emitter.StoreField(field);
            }

            emitter.LoadLocal(result);
            if (type.IsValueType) emitter.Box(type);
            emitter.Return();
            return emitter.CreateDelegate();
        }

        private SerializationManager.Serializer EmitSerializer(TypeInfo type, List<FieldInfo> fields)
        {
            var emitter = Emit<SerializationManager.Serializer>.NewDynamicMethod(type, type.Name + "Serializer");

            // Declare local variables.
            var typedInput = emitter.DeclareLocal(type);

            // Set the typed input variable from the method parameter.
            emitter.LoadArgument(0);
            emitter.CastOrUnbox(type);
            emitter.StoreLocal(typedInput);

            // Serialize each field
            foreach (var field in fields)
            {
                emitter.LoadLocal(typedInput);
                emitter.LoadField(field);
                if (field.FieldType.IsValueType) emitter.Box(field.FieldType);
                emitter.LoadArgument(1);
                emitter.LoadConstant(field.FieldType);
                emitter.Call(this.getTypeFromHandleMethodInfo);
                emitter.Call(this.serializeInnerMethodInfo);
            }

            emitter.Return();
            return emitter.CreateDelegate();
        }

        private SerializationManager.Deserializer EmitDeserializer(TypeInfo type, List<FieldInfo> fields)
        {
            var emitter = Emit<SerializationManager.Deserializer>.NewDynamicMethod(type, type.Name + "Deserializer");

            // Declare local variables.
            var result = emitter.DeclareLocal(type);

            // Construct the result.
            this.CreateInstance(emitter, type, result);

            // Record the object.
            emitter.Call(this.getCurrentDeserializationContext);
            emitter.LoadLocal(result);
            if (type.IsValueType) emitter.Box(type);
            emitter.Call(this.recordObjectWhileDeserializingMethodInfo);

            // Deserialize each field.
            foreach (var field in fields)
            {
                emitter.LoadLocalAsReference(type, result);
                emitter.LoadConstant(field.FieldType);
                emitter.Call(this.getTypeFromHandleMethodInfo);
                emitter.LoadArgument(1);
                emitter.Call(this.deserializeInnerMethodInfo);
                emitter.CastOrUnbox(field.FieldType);
                emitter.StoreField(field);
            }

            emitter.LoadLocal(result);
            if (type.IsValueType) emitter.Box(type);
            emitter.Return();
            return emitter.CreateDelegate();
        }

        private void CreateInstance<T>(Emit<T> emitter, TypeInfo type, Local result)
        {
            var constructorInfo = type.GetConstructor(Type.EmptyTypes);
            if (type.IsValueType)
            {
                emitter.LoadLocalAddress(result);
                emitter.InitializeObject(type);
            }
            else if (constructorInfo != null)
            {
                // Use the default constructor.
                emitter.NewObject(constructorInfo);
                emitter.StoreLocal(result);
            }
            else
            {
                emitter.LoadConstant(type);
                emitter.Call(this.getTypeFromHandleMethodInfo);
                emitter.Call(this.getUninitializedObjectMethodInfo);
                emitter.CastClass(type);
                emitter.StoreLocal(result);
            }
        }

        /// <summary>
        /// Returns a sorted list of the fields of the provided type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>A sorted list of the fields of the provided type.</returns>
        private List<FieldInfo> GetFields(TypeInfo type)
        {
            var result = type.GetAllFields().Where(field => !field.IsNotSerialized && !field.IsStatic && !field.FieldType.IsPointer).ToList();
            result.Sort(FieldInfoComparer.Instance);
            return result;
        }

        /// <summary>
        /// A comparer for <see cref="FieldInfo"/> which compares by name.
        /// </summary>
        private class FieldInfoComparer : IComparer<FieldInfo>
        {
            /// <summary>
            /// Gets the singleton instance of this class.
            /// </summary>
            public static FieldInfoComparer Instance { get; } = new FieldInfoComparer();

            public int Compare(FieldInfo x, FieldInfo y)
            {
                return string.Compare(x.Name, y.Name, StringComparison.Ordinal);
            }
        }

        [Serializable]
        public class IlBasedSerializerException : OrleansException
        {
            public IlBasedSerializerException()
            {
            }

            public IlBasedSerializerException(string message) : base(message)
            {
            }

            public IlBasedSerializerException(string message, Exception innerException) : base(message, innerException)
            {
            }

            protected IlBasedSerializerException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
            }
        }
    }

    internal static class EmitExtensions
    {
        public static void LoadLocalAsReference<T>(this Emit<T> emitter, TypeInfo type, Local result)
        {
            if (type.IsValueType)
            {
                emitter.LoadLocalAddress(result);
            }
            else
            {
                emitter.LoadLocal(result);
            }
        }

        public static void CastOrUnbox<T>(this Emit<T> emitter, Type type)
        {
            if (type.IsValueType) emitter.UnboxAny(type);
            else emitter.CastClass(type);
        }
    }
}