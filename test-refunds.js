// Test script to check refunds collection
const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);

async function checkRefunds() {
  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');
    
    const db = client.db('technotable');
    const refundsCollection = db.collection('refunds');
    
    // Count total refunds
    const count = await refundsCollection.countDocuments();
    console.log(`\n📊 Total refund records: ${count}`);
    
    if (count > 0) {
      // Get all refunds
      const refunds = await refundsCollection.find({}).toArray();
      
      console.log('\n📋 Refund Records:\n');
      refunds.forEach((refund, index) => {
        console.log(`\n${index + 1}. Refund ID: ${refund._id}`);
        console.log(`   User: ${refund.userName} (ID: ${refund.userId})`);
        console.log(`   Voucher ID: ${refund.voucherId}`);
        console.log(`   Package: ${refund.packageName}`);
        console.log(`   Status: ${refund.status}`);
        console.log(`   Total Refunded: Rs ${refund.totalRefundedAmount}`);
        console.log(`   Created: ${refund.createdAt}`);
        console.log(`   Refunded Months:`);
        refund.refundedMonths.forEach(month => {
          console.log(`      - ${month.month}: Rs ${month.refundedAmount}`);
        });
      });
    } else {
      console.log('\n⚠️  No refund records found in database');
      console.log('\nPossible reasons:');
      console.log('1. Backend may not be running');
      console.log('2. isRefund flag not being sent from frontend');
      console.log('3. No refunds have been processed yet');
      console.log('\nTry:');
      console.log('- Check backend console logs when clicking refund');
      console.log('- Look for "🔄 Refund operation detected" message');
      console.log('- Verify API call in browser Network tab');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.close();
    console.log('\n✅ Disconnected from MongoDB');
  }
}

checkRefunds();
