namespace Orleans.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.Serialization;

    using Orleans.Runtime;

    using Sigil;
    using Sigil.NonGeneric;

    internal class IlBasedSerializer
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
        /// A reference to <see cref="SerializationManager.DeepCopyInner"/>
        /// </summary>
        private readonly MethodInfo deepCopyInnerMethodInfo;

        /// <summary>
        /// A reference to the <see cref="SerializationContext.Current"/> getter method.
        /// </summary>
        private readonly MethodInfo getCurrentSerializationContext;

        /// <summary>
        /// A reference to the <see cref="SerializationContext.RecordObject(object, object)"/> method.
        /// </summary>
        private readonly MethodInfo recordObjectMethodInfo;

        public SerializationManager.SerializerMethods GenerateSerializer(TypeInfo type)
        {
            var fields = GetFields(type);
            SerializationManager.DeepCopier deepCopyDelegate = GetDeepCopier(type, fields);
            SerializationManager.Serializer serializeDelegate = null;
            SerializationManager.Deserializer deserializeDelegate = null;

            return new SerializationManager.SerializerMethods(deepCopyDelegate, serializeDelegate, deserializeDelegate);
        }

        private SerializationManager.DeepCopier GetDeepCopier(TypeInfo type, List<FieldInfo> fields)
        {
            var typeInfo = type.GetTypeInfo();

            //var method = new DynamicMethod(type + "DeepCopier", typeof(object), new[] { typeof(object) }, type.GetTypeInfo().Module, true);
            var emitter = Emit<SerializationManager.DeepCopier>.NewDynamicMethod(type, type.Name + "DeepCopier");//method.GetILGenerator();
            
            // Declare local variables.
            var result = emitter.DeclareLocal(type);
            var typedInput = emitter.DeclareLocal(type);

            // Set the typed input variable from the method parameter.
            emitter.LoadArgument(0);
            emitter.CastClass(type);
            emitter.StoreLocal(typedInput);

            // Construct the result.
            var constructorInfo = type.GetConstructor(Type.EmptyTypes);
            if (typeInfo.IsValueType)
            {
                emitter.LoadLocal(result);
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
                emitter.Call(this.getUninitializedObjectMethodInfo);
                emitter.CastClass(type);
                emitter.StoreLocal(result);
            }

            // Record the object.
            emitter.Call(this.getCurrentSerializationContext);
            emitter.LoadArgument(0); // Load 'original' parameter.
            emitter.LoadLocal(result); // Load 'result' local.
            emitter.Call(this.recordObjectMethodInfo);
            
            // Copy each field.
            foreach (var field in fields)
            {
                if (field.FieldType.IsOrleansShallowCopyable())
                {
                    emitter.LoadLocal(result);
                    emitter.LoadLocal(typedInput);
                    emitter.LoadField(field);
                    emitter.StoreField(field);
                }
                else
                {
                    emitter.LoadLocal(result);
                    emitter.LoadLocal(typedInput);
                    emitter.LoadField(field);
                    emitter.Call(this.deepCopyInnerMethodInfo);
                    emitter.CastClass(field.FieldType);
                    emitter.StoreField(field);
                }
            }

            emitter.LoadLocal(result);
            emitter.Return();
            return emitter.CreateDelegate();
        }

        public IlBasedSerializer()
        {
#if NETSTANDARD
            this.getUninitializedObjectMethodInfo = TypeUtils.Method(() => SerializationManager.GetUninitializedObjectWithFormatterServices(typeof(int)));
#else
            this.getUninitializedObjectMethodInfo = TypeUtils.Method(() => FormatterServices.GetUninitializedObject(typeof(int)));
#endif
            this.getTypeFromHandleMethodInfo = TypeUtils.Method(() => Type.GetTypeFromHandle(typeof(int).TypeHandle));

            this.deepCopyInnerMethodInfo = TypeUtils.Method(() => SerializationManager.DeepCopyInner(typeof(int)));
            this.getCurrentSerializationContext = TypeUtils.Property((object _) => SerializationContext.Current).GetMethod;
            this.recordObjectMethodInfo = TypeUtils.Method((SerializationContext ctx) => ctx.RecordObject(typeof(object), typeof(object)));
        }

        /// <summary>
        /// Returns a sorted list of the fields of the provided type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>A sorted list of the fields of the provided type.</returns>
        private List<FieldInfo> GetFields(TypeInfo type)
        {
            var result =
                type.GetAllFields()
                    .Where(field => !field.IsNotSerialized)
                    .ToList();
            result.Sort(FieldInfoComparer.Instance);
            return result;
        }

        /// <summary>
        /// A comparer for <see cref="FieldInfo"/> which compares by name.
        /// </summary>
        public class FieldInfoComparer : IComparer<FieldInfo>
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
    }
}