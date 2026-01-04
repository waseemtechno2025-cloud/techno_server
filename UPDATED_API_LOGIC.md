# Updated API Logic for Date Filter (Based on Vouchers)

## Current Voucher Structure:
```json
{
  "_id": "68feddda2a432fe2906e4657",
  "userId": "68feddd92a432fe2906e4656",
  "userName": "AAMIR ALI MACHI",
  "months": [
    {
      "month": "October 2025",
      "packageFee": 1500,
      "paidAmount": 1500,
      "remainingAmount": 0,
      "status": "paid",
      "date": "2025-10-27T02:50:02.293Z"
    },
    {
      "month": "November 2025",
      "packageFee": 1500,
      "paidAmount": 0,
      "remainingAmount": 1500,
      "status": "unpaid",
      "date": "2025-10-27T02:50:02.947Z"
    }
  ]
}
```

## Updated Logic:

### 1. GET /api/users/unpaid (with date filter)
**Steps**:
1. Get query params: `page`, `limit`, `expiryDate`
2. If `expiryDate` provided:
   - Query vouchers collection
   - Filter vouchers where months array has at least one month with matching date
   - Get userIds from filtered vouchers
   - Query users collection with those userIds
3. Apply pagination
4. Return users with totalCount

### 2. MongoDB Query for Vouchers:
```javascript
// Date filter on months.date field
const targetDate = new Date(expiryDate);
const startOfDay = new Date(targetDate.setHours(0, 0, 0, 0));
const endOfDay = new Date(targetDate.setHours(23, 59, 59, 999));

const vouchersWithMatchingDate = await vouchersCollection.find({
  'months.date': {
    $gte: startOfDay.toISOString(),
    $lte: endOfDay.toISOString()
  }
}).toArray();

// Get userIds from vouchers
const userIds = vouchersWithMatchingDate.map(v => v.userId);

// Query users
const users = await usersCollection.find({
  _id: { $in: userIds.map(id => new ObjectId(id)) },
  status: 'unpaid',
  $or: [
    { serviceStatus: { $ne: 'inactive' } },
    { serviceStatus: { $exists: false } }
  ]
})
.skip((page - 1) * limit)
.limit(limit)
.toArray();
```

### 3. Alternative: Filter by remainingAmount > 0 in matching months
```javascript
// More specific: only show users who have remainingAmount > 0 on that date
const vouchersWithPendingPayments = await vouchersCollection.aggregate([
  {
    $match: {
      'months.date': {
        $gte: startOfDay.toISOString(),
        $lte: endOfDay.toISOString()
      }
    }
  },
  {
    $project: {
      userId: 1,
      userName: 1,
      months: {
        $filter: {
          input: '$months',
          as: 'month',
          cond: {
            $and: [
              { $gte: ['$$month.date', startOfDay.toISOString()] },
              { $lte: ['$$month.date', endOfDay.toISOString()] },
              { $gt: ['$$month.remainingAmount', 0] }
            ]
          }
        }
      }
    }
  },
  {
    $match: {
      'months.0': { $exists: true } // Only vouchers with at least one matching month
    }
  }
]).toArray();
```

## Implementation:
The API should filter based on vouchers collection's months.date field, not user's expiryDate field.
