using Orleans.Serialization;
using Orleans.UnitTest.GrainInterfaces;
using Xunit;

// ReSharper disable NotAccessedVariable

namespace UnitTests.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using UnitTests.GrainInterfaces;

    /// <summary>
    /// Test the built-in serializers
    /// </summary>
    public class SerializerGenerationTests
    {
        public SerializerGenerationTests()
        {
            SerializationManager.InitializeForTesting();
        }

        [Fact, TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_TypeWithInternalNestedClass()
        {
            var v = new MyTypeWithAnInternalTypeField();

            Assert.NotNull(SerializationManager.GetSerializer(typeof (MyTypeWithAnInternalTypeField)));
            Assert.NotNull(SerializationManager.GetSerializer(typeof(MyTypeWithAnInternalTypeField.MyInternalDependency)));
        }

        [Fact, TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_ILBasedSerializer_Class()
        {
            var generator = new IlBasedSerializer();

            var input = OuterClass.GetPrivateClassInstance();
            input.Int = 89;
            input.String = Guid.NewGuid().ToString();
            input.NonSerializedInt = 39;
            input.Classes = new List<SomeAbstractClass>
            {
                input,
                new AnotherConcreteClass
                {
                    AnotherString = "hi",
                    Interfaces = new List<ISomeInterface> { input }
                }
            };

            // Set fields which should not be serialized.
#pragma warning disable 618
            input.ObsoleteInt = 38;
#pragma warning restore 618

            var methods = generator.GenerateSerializer(input.GetType().GetTypeInfo());
            var output = (SomeAbstractClass)this.SerializationLoop(input, methods);

            Assert.Equal(input.Int, output.Int);
            Assert.Equal(input.String, ((OuterClass.SomeConcreteClass)output).String);
            Assert.Equal(input.Classes.Count, output.Classes.Count);
            Assert.Equal(input.String, ((OuterClass.SomeConcreteClass)output.Classes[0]).String);
            Assert.Equal(input.Classes[1].Interfaces[0].Int, output.Classes[1].Interfaces[0].Int);
            Assert.Equal(0, output.NonSerializedInt);
#pragma warning disable 618
            Assert.Equal(input.ObsoleteInt, output.ObsoleteInt);
#pragma warning restore 618
        }

        [Fact, TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_ILBasedSerializer_Struct()
        {
            var generator = new IlBasedSerializer();
            
            // Test struct serialization.
            var expectedStruct = new SomeStruct(10) { Id = Guid.NewGuid(), PublicValue = 6, ValueWithPrivateGetter = 7 };
            expectedStruct.SetValueWithPrivateSetter(8);
            expectedStruct.SetPrivateValue(9);
            var methods = generator.GenerateSerializer(expectedStruct.GetType().GetTypeInfo());
            var actualStruct = (SomeStruct)this.SerializationLoop(expectedStruct, methods);
            Assert.Equal(expectedStruct.Id, actualStruct.Id);
            Assert.Equal(expectedStruct.ReadonlyField, actualStruct.ReadonlyField);
            Assert.Equal(expectedStruct.PublicValue, actualStruct.PublicValue);
            Assert.Equal(expectedStruct.ValueWithPrivateSetter, actualStruct.ValueWithPrivateSetter);
            Assert.Equal(expectedStruct.GetPrivateValue(), actualStruct.GetPrivateValue());
            Assert.Equal(expectedStruct.GetValueWithPrivateGetter(), actualStruct.GetValueWithPrivateGetter());
        }

        private object SerializationLoop(object input, SerializationManager.SerializerMethods methods, bool includeWire = true)
        {
            var copy = methods.DeepCopy(input);
            if (includeWire)
            {
                copy = RoundTripSerializationForTesting(copy, methods);
            }

            return copy;
        }
        
        /// <summary>
         /// Internal test method to do a round-trip Serialize+Deserialize loop
         /// </summary>
        public static T RoundTripSerializationForTesting<T>(T source, SerializationManager.SerializerMethods methods)
        {
            var data = SerializeToByteArray(source, methods.Serialize);
            return DeserializeFromByteArray<T>(data, methods.Deserialize);
        }

        /// <summary>
        /// Serialize data into byte[].
        /// </summary>
        /// <param name="raw">Input data.</param>
        /// <returns>Object of the required Type, rehydrated from the input stream.</returns>
        public static byte[] SerializeToByteArray(object raw, SerializationManager.Serializer serializer)
        {
            var stream = new BinaryTokenStreamWriter();
            byte[] result;
            try
            {
                SerializationContext.Current.Reset();
                serializer(raw, stream, null);
                SerializationContext.Current.Reset();
                result = stream.ToByteArray();
            }
            finally
            {
                stream.ReleaseBuffers();
            }
            return result;
        }
        
        /// <summary>
         /// Deserialize data from the specified byte[] and rehydrate backi into objects.
         /// </summary>
         /// <typeparam name="T">Type of data to be returned.</typeparam>
         /// <param name="data">Input data.</param>
         /// <returns>Object of the required Type, rehydrated from the input stream.</returns>
        public static T DeserializeFromByteArray<T>(byte[] data, SerializationManager.Deserializer deserializer)
        {
            var stream = new BinaryTokenStreamReader(data);
            DeserializationContext.Current.Reset();
            var result = deserializer(typeof(T), stream);
            DeserializationContext.Current.Reset();
            return (T)result;
        }
    }
}
