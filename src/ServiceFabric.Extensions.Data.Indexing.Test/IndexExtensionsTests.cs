using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.ServiceFabric.Data;
using ServiceFabric.Extensions.Data.Indexing.Persistent.Test.Models;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent.Test
{
	[TestClass]
	public class IndexExtensionsTests
	{
		[TestMethod]
		public async Task TryGetIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync<int, string>("test");

			Assert.IsFalse(result.HasValue);
			Assert.IsNull(result.Value);
			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task TryGetIndexed_NoIndexes_StateAdded()
		{
			var stateManager = new MockReliableStateManager();

			await stateManager.GetOrAddIndexedAsync<int, string>("test");

			var result = await stateManager.TryGetIndexedAsync<int, string>("test");

			Assert.IsTrue(result.HasValue);
			Assert.IsNotNull(result.Value);
			Assert.AreEqual(1, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task TryGetIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.IsFalse(result.HasValue);
			Assert.IsNull(result.Value);
			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task TryGetIndexed_OneIndex_StateAdded()
		{
			var stateManager = new MockReliableStateManager();

			await stateManager.GetOrAddIndexedAsync<Guid, Person>("someTest",
				new FilterableIndex<Guid, Person, string>("someIndex", (k, v) => v.Name));

			var result = await stateManager.TryGetIndexedAsync<Guid, Person>("someTest",
				new FilterableIndex<Guid, Person, string>("someIndex", (k, v) => v.Name));

			Assert.IsTrue(result.HasValue);
			Assert.IsNotNull(result.Value);
			Assert.AreEqual(2, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task TryGetIndexed_TwoIndexes_StateAdded()
		{
			var stateManager = new MockReliableStateManager();

			await stateManager.GetOrAddIndexedAsync<Guid, Person>("sometest",
				new FilterableIndex<Guid, Person, string>("someindex", (k, v) => v.Name),
				new FilterableIndex<Guid, Person, (int age, string city)>("someotherindex", (k, v) => (v.Age, v.Address.City)));

			var result = await stateManager.TryGetIndexedAsync<Guid, Person>("sometest",
				new FilterableIndex<Guid, Person, string>("someindex", (k, v) => v.Name),
				new FilterableIndex<Guid, Person, (int age, string city)>("someotherindex", (k, v) => (v.Age, v.Address.City)));

			Assert.IsTrue(result.HasValue);
			Assert.IsNotNull(result.Value);
			Assert.AreEqual(3, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task GetOrAddIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync<int, string>("test");

			Assert.IsNotNull(dictionary);
			Assert.AreEqual(1, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task GetOrAddIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.IsNotNull(dictionary);
			Assert.AreEqual(2, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task RemoveIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			await stateManager.GetOrAddIndexedAsync<int, string>("test");
			await stateManager.RemoveIndexedAsync<int, string>("test");

			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task RemoveIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));
			await stateManager.RemoveIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		private static async Task<int> GetReliableStateCountAsync(IReliableStateManager stateManager)
		{
			int count = 0;

			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(CancellationToken.None))
				count++;

			return count;
		}
	}
}
