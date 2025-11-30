require('dotenv').config();
const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
const cors = require('cors');
const cron = require('node-cron');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// MongoDB connection
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = 'street_management';
let db;
let streetsCollection;
let usersCollection;
let packagesCollection;
let employeesCollection;
let equipmentCollection;
let remindersCollection;
let routersCollection;
let fiberCablesCollection;
let vouchersCollection;
let transactionsCollection;
let expensesCollection;
let monthlySalesCollection;
let complaintsCollection;
let notificationsCollection;
let incomesCollection;
let isConnected = false;
let client;

// PKT Timezone constant (UTC+05:00)
const PKT_OFFSET_MIN = 5 * 60;

// Connect to MongoDB with connection reuse for serverless
async function connectToDatabase() {
  if (isConnected && db) {
    console.log('Using existing database connection');
    return db;
  }

  try {
    // Validate MongoDB URI
    if (!MONGODB_URI) {
      throw new Error('MONGODB_URI environment variable is not set');
    }
    
    console.log('Creating new database connection...');
    console.log('MONGODB_URI exists:', !!MONGODB_URI);
    
    client = await MongoClient.connect(MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      serverSelectionTimeoutMS: 10000, // 10 second timeout
    });
    
    console.log('MongoDB client connected');
    
    db = client.db(DB_NAME);
    streetsCollection = db.collection('streets');
    usersCollection = db.collection('users');
    packagesCollection = db.collection('packages');
    employeesCollection = db.collection('employees');
    equipmentCollection = db.collection('equipment');
    remindersCollection = db.collection('reminders');
    routersCollection = db.collection('routers');
    fiberCablesCollection = db.collection('fiberCables');
    vouchersCollection = db.collection('vouchers');
    transactionsCollection = db.collection('transactions');
    expensesCollection = db.collection('expenses');
    monthlySalesCollection = db.collection('monthlySales');
    complaintsCollection = db.collection('complaints');
    notificationsCollection = db.collection('notifications');
    incomesCollection = db.collection('incomes');
    
    console.log('Collections initialized:', {
      streets: !!streetsCollection,
      users: !!usersCollection,
      packages: !!packagesCollection,
      employees: !!employeesCollection,
      equipment: !!equipmentCollection,
      reminders: !!remindersCollection,
      routers: !!routersCollection,
      fiberCables: !!fiberCablesCollection,
      vouchers: !!vouchersCollection,
      transactions: !!transactionsCollection,
      expenses: !!expensesCollection,
      monthlySales: !!monthlySalesCollection,
      complaints: !!complaintsCollection,
      notifications: !!notificationsCollection,
      incomes: !!incomesCollection
    });
    
    // Create unique index on name field
    await streetsCollection.createIndex({ name: 1 }, { unique: true }).catch(err => {
      console.log('Index may already exist:', err.message);
    });
    
    isConnected = true;
    console.log('MongoDB connected successfully');
    
    // Initialize scheduled tasks after DB connection (only in non-serverless environment)
    if (process.env.NODE_ENV !== 'production' || !process.env.VERCEL) {
      initializeScheduledTasks();
    }
    
    return db;
  } catch (error) {
    console.error('MongoDB connection error:', error);
    console.error('Error details:', error.stack);
    throw error;
  }
}

// Middleware to ensure database connection
async function ensureDbConnection(req, res, next) {
  try {
    console.log(`[${req.method}] ${req.path} - Connection: ${isConnected}, DB: ${!!db}, Users: ${!!usersCollection}, Expenses: ${!!expensesCollection}`);
    
    if (!isConnected || !db || !usersCollection || !expensesCollection) {
      console.log('Database not connected or collections missing, connecting now...');
      await connectToDatabase();
      console.log('Database connected successfully in middleware');
    }
    
    // Double check core collections are initialized
    if (!usersCollection) {
      throw new Error('Users collection failed to initialize');
    }
    
    next();
  } catch (error) {
    console.error('Database connection error in middleware:', error);
    res.status(503).json({
      success: false,
      message: 'Database connection error',
      error: error.message
    });
  }
}

// Apply middleware to all routes
app.use(ensureDbConnection);

// Root route
app.get('/', (req, res) => {
  res.json({
    message: 'Street Management API',
    endpoints: {
      'POST /api/streets/add': 'Add a new street',
      'GET /api/streets': 'Get all streets',
      'GET /api/streets/:id': 'Get a street by ID',
      'DELETE /api/streets/:id': 'Delete a street'
    }
  });
});

// POST route to save a street name
app.post('/api/streets/add', async (req, res) => {
  try {
    const { name } = req.body;

    if (!name) {
      return res.status(400).json({
        success: false,
        message: 'Street name is required'
      });
    }

    // Check if street already exists
    const existingStreet = await streetsCollection.findOne({ name: name.trim() });
    if (existingStreet) {
      return res.status(409).json({
        success: false,
        message: 'Street name already exists'
      });
    }

    // Create new street
    const street = {
      name: name.trim(),
      createdAt: new Date()
    };

    const result = await streetsCollection.insertOne(street);
    
    res.status(201).json({
      success: true,
      message: 'Street saved successfully',
      data: {
        _id: result.insertedId,
        ...street
      }
    });
  } catch (error) {
    console.error('Error saving street:', error);
    res.status(500).json({
      success: false,
      message: 'Error saving street',
      error: error.message
    });
  }
});

// GET route to fetch all streets
app.get('/api/streets', async (req, res) => {
  try {
    const streets = await streetsCollection.find().sort({ createdAt: -1 }).toArray();
    res.status(200).json({
      success: true,
      count: streets.length,
      data: streets
    });
  } catch (error) {
    console.error('Error fetching streets:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching streets',
      error: error.message
    });
  }
});

// GET route to fetch a single street by ID
app.get('/api/streets/:id', async (req, res) => {
  try {
    const street = await streetsCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!street) {
      return res.status(404).json({
        success: false,
        message: 'Street not found'
      });
    }
    res.status(200).json({
      success: true,
      data: street
    });
  } catch (error) {
    console.error('Error fetching street:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching street',
      error: error.message
    });
  }
});

// PUT route to update a street
app.put('/api/streets/:id', async (req, res) => {
  try {
    const { name } = req.body;

    if (!name) {
      return res.status(400).json({
        success: false,
        message: 'Street name is required'
      });
    }

    const result = await streetsCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { name: name.trim() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Street not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Street updated successfully'
    });
  } catch (error) {
    console.error('Error updating street:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating street',
      error: error.message
    });
  }
});

// DELETE route to delete a street
app.delete('/api/streets/:id', async (req, res) => {
  try {
    const result = await streetsCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Street not found'
      });
    }
    res.status(200).json({
      success: true,
      message: 'Street deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting street:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting street',
      error: error.message
    });
  }
});

// ============ USERS API ROUTES ============

// POST route to add a new user
app.post('/api/users', async (req, res) => {
  try {
    const { 
      userName, 
      userId, 
      simNo, 
      whatsappNo, 
      packageName, 
      discount, 
      amount, 
      connectionType, 
      streetName, 
      switchSplitter, 
      assignTo, 
      feeCollector,
      rechargeDate, 
      expiryDate,
      status,
      paymentType,
      numberOfMonths
    } = req.body;

    if (!userName) {
      return res.status(400).json({
        success: false,
        message: 'User name is required'
      });
    }

    // Calculate total amount based on numberOfMonths
    const packageFeePerMonth = parseFloat(amount) || 0;
    const discountPerMonth = parseFloat(discount) || 0;
    const monthlyFeeAfterDiscount = packageFeePerMonth - discountPerMonth;
    const totalAmountForAllMonths = monthlyFeeAfterDiscount * (numberOfMonths || 1);
    
    console.log('💰 User Payment Calculation:', {
      packageFeePerMonth,
      discountPerMonth,
      monthlyFeeAfterDiscount,
      numberOfMonths,
      totalAmountForAllMonths,
      paymentType
    });
    
    // Parse expiry date to check if it's future
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();
    
    const parseExpiryDate = (expStr) => {
      if (!expStr) return null;
      const parts = String(expStr).split('-');
      if (parts.length === 3) {
        const d = parseInt(parts[0], 10);
        const m = parseInt(parts[1], 10) - 1;
        const y = parseInt(parts[2], 10);
        if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
          return { y, m, d };
        }
      }
      return null;
    };
    
    const expiryYMD = parseExpiryDate(expiryDate);
    const isFutureExpiry = expiryYMD && (
      expiryYMD.y > todayY ||
      (expiryYMD.y === todayY && expiryYMD.m > todayM) ||
      (expiryYMD.y === todayY && expiryYMD.m === todayM && expiryYMD.d > todayD)
    );
    
    console.log('📅 Expiry Date Check:', {
      expiryDate,
      expiryYMD,
      todayYMD: { y: todayY, m: todayM, d: todayD },
      isFutureExpiry
    });
    
    // Determine payment status
    let paymentStatus = 'unpaid'; // Default
    let paidAmount = 0;
    let remainingAmount = totalAmountForAllMonths;
    
    // Payment status logic:
    // - Pay Now: paid/partial (shows in Paid + Expiring Soon)
    // - Pay Later: always unpaid (shows in Unpaid Users)
    // - Checkbox: pending (shows in Expiring Soon only)
    if (status === 'pending') {
      // Explicit pending (checkbox)
      paymentStatus = 'pending';
      console.log('✅ Checkbox: pending status');
    } else if (paymentType === 'now') {
      // Pay Now: Always paid/partial (regardless of expiry date)
      paymentStatus = numberOfMonths > 1 ? 'partial' : 'paid';
      paidAmount = monthlyFeeAfterDiscount;
      remainingAmount = totalAmountForAllMonths - monthlyFeeAfterDiscount;
      console.log('✅ Pay Now: paid/partial status (shows in Paid + Expiring Soon)');
    } else if (paymentType === 'later') {
      // Pay Later: Always unpaid (shows in Unpaid Users)
      paymentStatus = 'unpaid';
      console.log('✅ Pay Later: unpaid status (shows in Unpaid Users)');
    }
    
    console.log('📊 Final Payment Status:', {
      paymentStatus,
      paidAmount,
      remainingAmount
    });
    
    // Check if expiry date is TODAY (before 12 PM) or TOMORROW - if yes, set showInExpiringSoon flag
    const currentHourPKT = nowInPKT.getUTCHours();
    
    const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
    const tomorrowY = tomorrowDate.getUTCFullYear();
    const tomorrowM = tomorrowDate.getUTCMonth();
    const tomorrowD = tomorrowDate.getUTCDate();
    
    const isExpiringToday = expiryYMD && 
      expiryYMD.y === todayY && 
      expiryYMD.m === todayM && 
      expiryYMD.d === todayD;
    
    const isExpiringTomorrow = expiryYMD && 
      expiryYMD.y === tomorrowY && 
      expiryYMD.m === tomorrowM && 
      expiryYMD.d === tomorrowD;
    
    // Set flag if:
    // 1. Expires tomorrow (always)
    // 2. Expires today BUT current time is before 12 PM
    const shouldShowInExpiringSoon = isExpiringTomorrow || (isExpiringToday && currentHourPKT < 12);
    
    if (shouldShowInExpiringSoon) {
      if (isExpiringToday && currentHourPKT < 12) {
        console.log('🔔 User expires TODAY (before 12 PM) - Setting showInExpiringSoon flag immediately');
      } else if (isExpiringTomorrow) {
        console.log('🔔 User expires TOMORROW - Setting showInExpiringSoon flag immediately');
      }
    }

    const newUser = {
      userName: userName.trim(),
      userId: userId ? userId.trim() : '',
      simNo: simNo ? simNo.trim() : '',
      whatsappNo: whatsappNo ? whatsappNo.trim() : '',
      packageName: packageName || '',
      discount: discount || 0,
      amount: amount || 0, // Single month package fee
      totalAmount: totalAmountForAllMonths, // Total for all months
      numberOfMonths: numberOfMonths || 1,
      connectionType: connectionType || 'Local',
      streetName: streetName || '',
      switchSplitter: switchSplitter ? switchSplitter.trim() : '',
      assignTo: assignTo ? assignTo.trim() : '',
      feeCollector: feeCollector ? feeCollector.trim() : '',
      rechargeDate: rechargeDate || null,
      expiryDate: expiryDate || null,
      status: paymentStatus, // Payment status: paid, unpaid, partial, pending
      serviceStatus: 'active', // Service status: always active for new users
      paidAmount: paidAmount,
      remainingAmount: remainingAmount,
      unpaidSince: paymentStatus === 'unpaid' ? new Date() : null,
      showInExpiringSoon: shouldShowInExpiringSoon, // Set if expires today (before 12 PM) or tomorrow
      createdAt: new Date()
    };

    const result = await usersCollection.insertOne(newUser);
    const newUserId = result.insertedId;
    
    // Voucher creation functionality has been removed from this endpoint
    // Vouchers will be created separately through the dedicated voucher endpoint
    // VoucherModal will handle all voucher creation including expired users
    
    res.status(201).json({
      success: true,
      message: 'User added successfully',
      data: { _id: newUserId, ...newUser }
    });
  } catch (error) {
    console.error('Error adding user:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding user',
      error: error.message
    });
  }
});

// GET route to fetch all users (with optional search)
app.get('/api/users', async (req, res) => {
  try {
    // Explicit check for collection initialization
    if (!usersCollection) {
      console.error('usersCollection is undefined! DB connection status:', isConnected);
      return res.status(503).json({
        success: false,
        message: 'Database not initialized',
        error: 'Collections are not ready'
      });
    }
    
    // Check if search query is provided
    const searchQuery = req.query.search;
    const feeCollector = req.query.feeCollector; // Fee collector filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    
    let query = {};
    
    if (searchQuery) {
      // Search in userName, userId, and phoneNumber fields (case-insensitive)
      query = {
        $or: [
          { userName: { $regex: searchQuery, $options: 'i' } },
          { userId: { $regex: searchQuery, $options: 'i' } },
          { phoneNumber: { $regex: searchQuery, $options: 'i' } }
        ]
      };
    }
    
    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      if (feeCollectorTrimmed) {
      query.feeCollector = { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') };
        console.log(`🔒 STRICT: Filtering /api/users by fee collector (case-insensitive): ${feeCollectorTrimmed}`);
      }
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
      query.assignTo = { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') };
        console.log(`🔒 STRICT: Filtering /api/users by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    const users = await usersCollection.find(query).sort({ userName: 1 }).toArray();
    res.status(200).json({
      success: true,
      count: users.length,
      data: users
    });
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching users',
      error: error.message
    });
  }
});

// PUT route to update a user
app.put('/api/users/:id', async (req, res) => {
  try {
    // Validate ObjectId format
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid user ID format'
      });
    }

    const { 
      name, 
      userName,
      userId, 
      simNo, 
      whatsappNo, 
      packageName, 
      discount, 
      amount, 
      connectionType, 
      streetName, 
      switchSplitter, 
      assignTo, 
      feeCollector,
      rechargeDate, 
      expiryDate, 
      networkType,
      status,
      serviceStatus,
      paidAmount,
      remainingAmount
    } = req.body;

    // Build update object dynamically
    const updateFields = {};
    
    if (name) updateFields.name = name.trim();
    if (userName) updateFields.userName = userName.trim();
    if (userId !== undefined) updateFields.userId = userId ? userId.trim() : '';
    if (simNo !== undefined) updateFields.simNo = simNo ? simNo.trim() : '';
    if (whatsappNo !== undefined) updateFields.whatsappNo = whatsappNo ? whatsappNo.trim() : '';
    if (packageName !== undefined) updateFields.packageName = packageName || '';
    if (discount !== undefined) updateFields.discount = discount || 0;
    if (amount !== undefined) updateFields.amount = amount || 0;
    if (connectionType !== undefined) updateFields.connectionType = connectionType || '';
    if (streetName !== undefined) updateFields.streetName = streetName || '';
    if (switchSplitter !== undefined) updateFields.switchSplitter = switchSplitter ? switchSplitter.trim() : '';
    if (assignTo !== undefined) updateFields.assignTo = assignTo ? assignTo.trim() : '';
    if (feeCollector !== undefined) updateFields.feeCollector = feeCollector ? feeCollector.trim() : '';
    if (rechargeDate !== undefined) updateFields.rechargeDate = rechargeDate || null;
    if (expiryDate !== undefined) updateFields.expiryDate = expiryDate || null;
    if (networkType !== undefined) updateFields.networkType = networkType || 'local';
    if (status !== undefined) updateFields.status = status;
    if (serviceStatus !== undefined) updateFields.serviceStatus = serviceStatus;
    if (paidAmount !== undefined) updateFields.paidAmount = paidAmount || 0;
    if (remainingAmount !== undefined) updateFields.remainingAmount = remainingAmount || 0;

    // If status is being changed, manage unpaidSince timestamp
    if (status !== undefined) {
      // Fetch current user to detect transitions
      const existingUser = await usersCollection.findOne(
        { _id: new ObjectId(req.params.id) },
        { projection: { status: 1, unpaidSince: 1 } }
      );
      if (status === 'unpaid') {
        // Set unpaidSince only if not already set
        if (!existingUser?.unpaidSince) updateFields.unpaidSince = new Date();
      } else {
        // Clearing unpaid state, remove timestamp
        if (existingUser?.unpaidSince) updateFields.unpaidSince = null;
      }
    }

    if (Object.keys(updateFields).length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }
    
    // Check if expiry date is being updated and if it's TODAY (before 12 PM) or TOMORROW
    if (expiryDate !== undefined) {
      const nowUTC = new Date();
      const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
      const todayY = nowInPKT.getUTCFullYear();
      const todayM = nowInPKT.getUTCMonth();
      const todayD = nowInPKT.getUTCDate();
      const currentHourPKT = nowInPKT.getUTCHours();
      
      const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
      const tomorrowY = tomorrowDate.getUTCFullYear();
      const tomorrowM = tomorrowDate.getUTCMonth();
      const tomorrowD = tomorrowDate.getUTCDate();
      
      // Parse expiry date
      const parseExpiryDate = (expStr) => {
        if (!expStr) return null;
        const parts = String(expStr).split('-');
        if (parts.length === 3) {
          const d = parseInt(parts[0], 10);
          const m = parseInt(parts[1], 10) - 1;
          const y = parseInt(parts[2], 10);
          if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
            return { y, m, d };
          }
        }
        return null;
      };
      
      const expiryYMD = parseExpiryDate(expiryDate);
      
      const isExpiringToday = expiryYMD && 
        expiryYMD.y === todayY && 
        expiryYMD.m === todayM && 
        expiryYMD.d === todayD;
      
      const isExpiringTomorrow = expiryYMD && 
        expiryYMD.y === tomorrowY && 
        expiryYMD.m === tomorrowM && 
        expiryYMD.d === tomorrowD;
      
      // Set flag if:
      // 1. Expires tomorrow (always)
      // 2. Expires today BUT current time is before 12 PM
      const shouldShowInExpiringSoon = isExpiringTomorrow || (isExpiringToday && currentHourPKT < 12);
      
      if (shouldShowInExpiringSoon) {
        if (isExpiringToday && currentHourPKT < 12) {
          console.log('🔔 User expires TODAY (before 12 PM) - Setting showInExpiringSoon flag');
        } else if (isExpiringTomorrow) {
          console.log('🔔 User expires TOMORROW - Setting showInExpiringSoon flag');
        }
        updateFields.showInExpiringSoon = true;
      } else {
        // If expiry date is not today/tomorrow, remove flag
        updateFields.showInExpiringSoon = false;
      }
    }

    const result = await usersCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updateFields }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    // If package name, amount, or discount is being updated, also update related vouchers
    if (packageName !== undefined || amount !== undefined || discount !== undefined) {
      try {
        console.log('📦 Updating vouchers for package change:', {
          userId: req.params.id,
          newPackageName: packageName,
          newAmount: amount
        });

        // Update vouchers that have unpaid months for this user
        const voucherUpdateFields = {};
        if (packageName !== undefined) voucherUpdateFields['months.$[elem].packageName'] = packageName;
        if (amount !== undefined) {
          voucherUpdateFields['months.$[elem].packageFee'] = amount;
        }
        
        // If amount or discount changed, recalculate remaining amount for unpaid months
        if (amount !== undefined || discount !== undefined) {
          // Get the current user data to have full context
          const currentUser = await usersCollection.findOne({ _id: new ObjectId(req.params.id) });
          const currentAmount = amount !== undefined ? amount : (currentUser?.amount || 0);
          const currentDiscount = discount !== undefined ? discount : (currentUser?.discount || 0);
          const finalAmount = currentAmount - currentDiscount;
          
          voucherUpdateFields['months.$[elem].packageFee'] = currentAmount;
          voucherUpdateFields['months.$[elem].discount'] = currentDiscount;
          voucherUpdateFields['months.$[elem].remainingAmount'] = finalAmount;
          
          console.log('💰 Updating voucher amounts:', {
            currentAmount,
            currentDiscount,
            finalAmount
          });
        }

        if (Object.keys(voucherUpdateFields).length > 0) {
          await vouchersCollection.updateMany(
            { userId: req.params.id },
            { $set: voucherUpdateFields },
            { 
              arrayFilters: [
                { 
                  $or: [
                    { 'elem.status': 'unpaid' },
                    { 'elem.remainingAmount': { $gt: 0 } }
                  ]
                }
              ]
            }
          );
          console.log('✅ Vouchers updated for package change');
        }
      } catch (voucherError) {
        console.error('⚠️ Error updating vouchers:', voucherError);
        // Don't fail the user update if voucher update fails
      }
    }

    res.status(200).json({
      success: true,
      message: 'User updated successfully'
    });
  } catch (error) {
    console.error('Error updating user:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating user',
      error: error.message
    });
  }
});

// DELETE route to delete a user
app.delete('/api/users/:id', async (req, res) => {
  try {
    // Validate ObjectId format
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid user ID format'
      });
    }

    const userId = new ObjectId(req.params.id);
    
    // STEP 1: Fetch all vouchers for this user BEFORE deleting
    const userVouchers = await vouchersCollection.find({ userId: req.params.id }).toArray();
    console.log(`🔍 Found ${userVouchers.length} vouchers for user ${req.params.id}`);
    
    // STEP 2: Calculate income to deduct based on payment method
    const incomeDeductions = {}; // { receiverName: { cashIncome: amount, bankIncome: amount } }
    
    for (const voucher of userVouchers) {
      if (voucher.months && Array.isArray(voucher.months)) {
        for (const month of voucher.months) {
          // Skip reversed/refunded months
          if (month.status === 'reversed' || month.refundDate || month.refundedAmount) {
            continue;
          }
          
          // Check payment history first (new structure)
          const paymentHistory = month.paymentHistory || [];
          
          if (paymentHistory.length > 0) {
            // New structure: Process each payment
            for (const payment of paymentHistory) {
              const receiver = payment.receivedBy || 'Admin';
              const amount = parseFloat(payment.amount) || 0;
              const paymentMethod = (payment.paymentMethod || '').trim().toLowerCase();
              
              if (amount > 0) {
                if (!incomeDeductions[receiver]) {
                  incomeDeductions[receiver] = { cashIncome: 0, bankIncome: 0 };
                }
                
                if (paymentMethod === 'cash') {
                  incomeDeductions[receiver].cashIncome += amount;
                } else if (paymentMethod === 'bank transfer') {
                  incomeDeductions[receiver].bankIncome += amount;
                }
                
                console.log(`   💰 Deduct: ${receiver} - ${paymentMethod}: Rs ${amount}`);
              }
            }
          } else if (month.receivedBy && (month.status === 'paid' || month.status === 'partial')) {
            // Old structure: Single receivedBy field
            const receiver = month.receivedBy;
            const amount = parseFloat(month.paidAmount) || 0;
            const paymentMethod = (month.paymentMethod || '').trim().toLowerCase();
            
            if (amount > 0) {
              if (!incomeDeductions[receiver]) {
                incomeDeductions[receiver] = { cashIncome: 0, bankIncome: 0 };
              }
              
              if (paymentMethod === 'cash') {
                incomeDeductions[receiver].cashIncome += amount;
              } else if (paymentMethod === 'bank transfer') {
                incomeDeductions[receiver].bankIncome += amount;
              } else {
                // If payment method not specified, deduct from cashIncome
                incomeDeductions[receiver].cashIncome += amount;
              }
              
              console.log(`   💰 Deduct (old): ${receiver} - ${paymentMethod || 'cash'}: Rs ${amount}`);
            }
          }
        }
      }
    }
    
    // STEP 3: Update income collection - deduct amounts (prevent negative values)
    for (const [receiver, deductions] of Object.entries(incomeDeductions)) {
      const { cashIncome, bankIncome } = deductions;
      
      if (cashIncome > 0 || bankIncome > 0) {
        // CRITICAL: Fetch current income first to prevent negative values
        const currentIncomeRecord = await incomesCollection.findOne({
          name: { $regex: new RegExp(`^${receiver.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }
        });
        
        if (currentIncomeRecord) {
          const currentCash = currentIncomeRecord.cashIncome || 0;
          const currentBank = currentIncomeRecord.bankIncome || 0;
          
          // Calculate new values (don't go below 0)
          const newCash = Math.max(0, currentCash - cashIncome);
          const newBank = Math.max(0, currentBank - bankIncome);
          
          await incomesCollection.updateOne(
            { name: { $regex: new RegExp(`^${receiver.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
            { 
              $set: { 
                cashIncome: newCash,
                bankIncome: newBank,
                lastUpdated: new Date() 
              }
            }
          );
          
          console.log(`✅ Deducted from ${receiver}: Cash Rs ${currentCash} → Rs ${newCash}, Bank Rs ${currentBank} → Rs ${newBank}`);
        } else {
          console.log(`⚠️ No income record found for ${receiver}, skipping deduction`);
        }
      }
    }
    
    // STEP 4: Delete all vouchers for this user
    const vouchersResult = await vouchersCollection.deleteMany({ userId: req.params.id });
    console.log(`🗑️ Deleted ${vouchersResult.deletedCount} vouchers for user ${req.params.id}`);
    
    // STEP 5: Delete the user
    const result = await usersCollection.deleteOne({ _id: userId });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'User, vouchers, and income adjustments completed successfully',
      deletedVouchersCount: vouchersResult.deletedCount,
      incomeDeductions: incomeDeductions
    });
  } catch (error) {
    console.error('Error deleting user:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting user',
      error: error.message
    });
  }
});

// ============ PACKAGES API ROUTES ============

// POST route to add a package
app.post('/api/packages/add', async (req, res) => {
  try {
    const { name, speed, price } = req.body;

    if (!name || !speed || !price) {
      return res.status(400).json({
        success: false,
        message: 'Name, speed, and price are required'
      });
    }

    const package = {
      name: name.trim(),
      speed: speed.trim(),
      price: parseFloat(price),
      createdAt: new Date()
    };

    const result = await packagesCollection.insertOne(package);
    
    res.status(201).json({
      success: true,
      message: 'Package added successfully',
      data: {
        _id: result.insertedId,
        ...package
      }
    });
  } catch (error) {
    console.error('Error adding package:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding package',
      error: error.message
    });
  }
});

// GET route to fetch all packages
app.get('/api/packages', async (req, res) => {
  try {
    const packages = await packagesCollection.find().sort({ createdAt: -1 }).toArray();
    res.status(200).json({
      success: true,
      count: packages.length,
      data: packages
    });
  } catch (error) {
    console.error('Error fetching packages:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching packages',
      error: error.message
    });
  }
});

// PUT route to update a package
app.put('/api/packages/:id', async (req, res) => {
  try {
    const { name, speed, price } = req.body;

    if (!name || !speed || !price) {
      return res.status(400).json({
        success: false,
        message: 'Name, speed, and price are required'
      });
    }

    const result = await packagesCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { name: name.trim(), speed: speed.trim(), price: parseFloat(price) } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Package updated successfully'
    });
  } catch (error) {
    console.error('Error updating package:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating package',
      error: error.message
    });
  }
});

// DELETE route to delete a package
app.delete('/api/packages/:id', async (req, res) => {
  try {
    const result = await packagesCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }
    res.status(200).json({
      success: true,
      message: 'Package deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting package:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting package',
      error: error.message
    });
  }
});

// ============ EMPLOYEES API ROUTES ============

// POST route to add an employee
app.post('/api/employees/add', async (req, res) => {
  try {
    const { name, number, role, salary, username, password, isActive, assignedCustomers } = req.body;

    if (!name || !number || !role || salary === undefined || !username || !password) {
      return res.status(400).json({
        success: false,
        message: 'Name, number, role, salary, username, and password are required'
      });
    }

    const employee = {
      name: name.trim(),
      number: number.trim(),
      role: role.trim(),
      salary: parseFloat(salary),
      username: username.trim(),
      password: password.trim(),
      isActive: isActive !== undefined ? isActive : true,
      assignedCustomers: assignedCustomers && Array.isArray(assignedCustomers) ? assignedCustomers : [],
      createdAt: new Date()
    };

    const result = await employeesCollection.insertOne(employee);
    const employeeId = result.insertedId;
    
    // Update assigned customers in users collection
    if (assignedCustomers && Array.isArray(assignedCustomers) && assignedCustomers.length > 0) {
      const employeeName = name.trim();
      const roleLower = role.trim().toLowerCase();
      
      // Update customers based on employee role
      if (roleLower === 'fee collector') {
        // Set feeCollector field for assigned customers
        await usersCollection.updateMany(
          { _id: { $in: assignedCustomers.map(id => new ObjectId(id)) } },
          { $set: { feeCollector: employeeName } }
        );
        console.log(`✅ Assigned ${assignedCustomers.length} customers to fee collector: ${employeeName}`);
      } else if (roleLower === 'technician') {
        // Set assignTo field for assigned customers
        await usersCollection.updateMany(
          { _id: { $in: assignedCustomers.map(id => new ObjectId(id)) } },
          { $set: { assignTo: employeeName } }
        );
        console.log(`✅ Assigned ${assignedCustomers.length} customers to technician: ${employeeName}`);
      }
    }
    
    res.status(201).json({
      success: true,
      message: 'Employee added successfully',
      data: {
        _id: employeeId,
        ...employee
      }
    });
  } catch (error) {
    console.error('Error adding employee:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding employee',
      error: error.message
    });
  }
});

// GET route to fetch all employees
app.get('/api/employees', async (req, res) => {
  try {
    const employees = await employeesCollection.find().sort({ createdAt: -1 }).toArray();
    res.status(200).json({
      success: true,
      count: employees.length,
      data: employees
    });
  } catch (error) {
    console.error('Error fetching employees:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching employees',
      error: error.message
    });
  }
});

// PUT route to update an employee
app.put('/api/employees/:id', async (req, res) => {
  try {
    const { name, number, role, salary, username, password, isActive, assignedCustomers } = req.body;

    if (!name || !number || !role || salary === undefined || !username || !password) {
      return res.status(400).json({
        success: false,
        message: 'Name, number, role, salary, username, and password are required'
      });
    }

    // Get existing employee to check previous assignments
    const existingEmployee = await employeesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!existingEmployee) {
      return res.status(404).json({
        success: false,
        message: 'Employee not found'
      });
    }

    const employeeName = name.trim();
    const roleLower = role.trim().toLowerCase();
    const previousAssignedCustomers = existingEmployee.assignedCustomers || [];
    const newAssignedCustomers = assignedCustomers && Array.isArray(assignedCustomers) ? assignedCustomers : [];

    // Update employee document
    const result = await employeesCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { 
        $set: { 
          name: employeeName, 
          number: number.trim(), 
          role: role.trim(), 
          salary: parseFloat(salary),
          username: username.trim(),
          password: password.trim(),
          isActive: isActive !== undefined ? isActive : true,
          assignedCustomers: newAssignedCustomers
        } 
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Employee not found'
      });
    }

    // Remove assignments from customers that were unassigned
    const unassignedCustomers = previousAssignedCustomers.filter(
      id => !newAssignedCustomers.includes(id)
    );
    
    if (unassignedCustomers.length > 0) {
      const previousRoleLower = (existingEmployee.role || '').toLowerCase();
      if (previousRoleLower === 'fee collector') {
        await usersCollection.updateMany(
          { _id: { $in: unassignedCustomers.map(id => new ObjectId(id)) } },
          { $unset: { feeCollector: '' } }
        );
      } else if (previousRoleLower === 'technician') {
        await usersCollection.updateMany(
          { _id: { $in: unassignedCustomers.map(id => new ObjectId(id)) } },
          { $unset: { assignTo: '' } }
        );
      }
    }

    // Update newly assigned customers
    if (newAssignedCustomers.length > 0) {
      if (roleLower === 'fee collector') {
        // Set feeCollector field for newly assigned customers
        await usersCollection.updateMany(
          { _id: { $in: newAssignedCustomers.map(id => new ObjectId(id)) } },
          { $set: { feeCollector: employeeName } }
        );
        console.log(`✅ Updated ${newAssignedCustomers.length} customers for fee collector: ${employeeName}`);
      } else if (roleLower === 'technician') {
        // Set assignTo field for newly assigned customers
        await usersCollection.updateMany(
          { _id: { $in: newAssignedCustomers.map(id => new ObjectId(id)) } },
          { $set: { assignTo: employeeName } }
        );
        console.log(`✅ Updated ${newAssignedCustomers.length} customers for technician: ${employeeName}`);
      }
    }

    res.status(200).json({
      success: true,
      message: 'Employee updated successfully'
    });
  } catch (error) {
    console.error('Error updating employee:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating employee',
      error: error.message
    });
  }
});

// DELETE route to delete an employee
app.delete('/api/employees/:id', async (req, res) => {
  try {
    const result = await employeesCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Employee not found'
      });
    }
    res.status(200).json({
      success: true,
      message: 'Employee deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting employee:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting employee',
      error: error.message
    });
  }
});

// POST route to authenticate employee login
app.post('/api/auth/login', ensureDbConnection, async (req, res) => {
  try {
    const { username, password, role } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        success: false,
        message: 'Username and password are required'
      });
    }

    // Ensure employeesCollection is initialized
    if (!employeesCollection) {
      console.error('❌ Employees collection not initialized');
      return res.status(500).json({
        success: false,
        message: 'Database connection error',
        error: 'Employees collection not initialized'
      });
    }

    // Find employee with matching username and password
    // Only allow active employees to login - strictly check isActive: true
    const employee = await employeesCollection.findOne({
      username: username.trim(),
      password: password.trim(),
      isActive: true // Only active employees can login
    });

    if (!employee) {
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password'
      });
    }

    // Optional: Validate role if provided
    if (role && employee.role && employee.role.toLowerCase() !== role.toLowerCase()) {
      return res.status(401).json({
        success: false,
        message: 'Invalid role for this user'
      });
    }

    // Return success with employee data (excluding password)
    const { password: _, ...employeeData } = employee;
    res.status(200).json({
      success: true,
      message: 'Login successful',
      data: employeeData
    });

  } catch (error) {
    console.error('❌ Error during login:', error);
    console.error('Error stack:', error.stack);
    console.error('Database connection status:', {
      isConnected,
      hasDb: !!db,
      hasEmployeesCollection: !!employeesCollection
    });
    
    res.status(500).json({
      success: false,
      message: 'Error during login',
      error: error.message
    });
  }
});

// ============ EQUIPMENT (SWITCH & SPLITTER) API ROUTES ============

// POST route to add equipment
app.post('/api/equipment/add', async (req, res) => {
  try {
    const { name, type, streetId, streetName, location, ports, usedPorts } = req.body;

    if (!name || !type || !streetId || !streetName || !location || ports === undefined || usedPorts === undefined) {
      return res.status(400).json({
        success: false,
        message: 'All fields are required'
      });
    }

    const equipment = {
      name: name.trim(),
      type: type,
      streetId: streetId,
      streetName: streetName.trim(),
      location: location.trim(),
      ports: parseInt(ports),
      usedPorts: parseInt(usedPorts),
      createdAt: new Date()
    };

    const result = await equipmentCollection.insertOne(equipment);
    
    res.status(201).json({
      success: true,
      message: 'Equipment added successfully',
      data: {
        _id: result.insertedId,
        ...equipment
      }
    });
  } catch (error) {
    console.error('Error adding equipment:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding equipment',
      error: error.message
    });
  }
});

// GET route to fetch all equipment
app.get('/api/equipment', async (req, res) => {
  try {
    // Support query params for suggestions
    // q: text search on name (case-insensitive)
    // streetId: filter by streetId if provided
    // type: optional filter (e.g., "Switch", "Splitter")
    const { q, streetId, type } = req.query;

    const query = {};
    if (q) {
      query.name = { $regex: String(q), $options: 'i' };
    }
    if (streetId) {
      query.streetId = String(streetId);
    }
    if (type) {
      query.type = String(type);
    }

    const cursor = equipmentCollection
      .find(query)
      .sort({ createdAt: -1 });

    // For suggestions, it's often useful to cap results
    if (q) {
      cursor.limit(20);
    }

    const equipment = await cursor.toArray();
    res.status(200).json({
      success: true,
      count: equipment.length,
      data: equipment
    });
  } catch (error) {
    console.error('Error fetching equipment:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching equipment',
      error: error.message
    });
  }
});

// PUT route to update equipment
app.put('/api/equipment/:id', async (req, res) => {
  try {
    const { name, type, streetId, streetName, location, ports, usedPorts } = req.body;

    if (!name || !type || !streetId || !streetName || !location || ports === undefined || usedPorts === undefined) {
      return res.status(400).json({
        success: false,
        message: 'All fields are required'
      });
    }

    const result = await equipmentCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { 
        $set: { 
          name: name.trim(), 
          type: type, 
          streetId: streetId,
          streetName: streetName.trim(),
          location: location.trim(), 
          ports: parseInt(ports),
          usedPorts: parseInt(usedPorts)
        } 
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Equipment not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Equipment updated successfully'
    });
  } catch (error) {
    console.error('Error updating equipment:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating equipment',
      error: error.message
    });
  }
});

// DELETE route to delete equipment
app.delete('/api/equipment/:id', async (req, res) => {
  try {
    const result = await equipmentCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Equipment not found'
      });
    }
    res.status(200).json({
      success: true,
      message: 'Equipment deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting equipment:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting equipment',
      error: error.message
    });
  }
});

// ============ DASHBOARD API ROUTES ============

// GET dashboard stats
app.get('/api/dashboard/stats', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const transactionsCollection = db.collection('transactions');
    const vouchersCol = db.collection('vouchers');
    
    // Get filter parameters
    const feeCollector = req.query.feeCollector;
    const assignTo = req.query.assignTo;
    
    // Build base user query filter
    let userFilter = {};
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      userFilter.feeCollector = { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') };
      console.log(`🔍 Filtering dashboard stats by feeCollector (case-insensitive): ${feeCollectorTrimmed}`);
    }
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      userFilter.assignTo = { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') };
      console.log(`🔍 Filtering dashboard stats by assignTo (case-insensitive): ${assignToTrimmed}`);
    }
    
    // Get user IDs that match the filter (for voucher filtering)
    // CRITICAL: For fee collector, we should NOT pre-filter by user.feeCollector
    // Instead, check receivedBy in vouchers directly (same as paid-users endpoint)
    // Only pre-filter for technician (assignTo) since that's a user-level field
    let filteredUserIds = null;
    if (assignTo) {
      // For technician, pre-filter by assignTo
      const filteredUsers = await usersCollection.find(userFilter).toArray();
      filteredUserIds = filteredUsers.map(u => u._id.toString());
      console.log(`📊 Found ${filteredUserIds.length} users matching assignTo filter`);
      
      // If filter is applied but no users match, return all zeros
      if (filteredUserIds.length === 0) {
        console.log(`⚠️ No users found for assignTo filter - returning zero stats`);
        return res.status(200).json({
          success: true,
          data: {
            totalUsers: 0,
            paidUsers: 0,
            totalIncome: 0,
            totalExpense: 0,
            unpaidUsers: 0,
            outstanding: 0,
            balance: 0,
            balanceCustomers: 0,
            expiringSoon: 0,
            deactivatedUsers: 0
          }
        });
      }
    } else if (feeCollector) {
      // For fee collector, don't pre-filter - we'll check receivedBy in vouchers directly
      console.log(`📊 Fee collector filter: Will check receivedBy in all vouchers (not pre-filtering by user.feeCollector)`);
    }
    
    // Total users (with filter if provided)
    // CRITICAL: Count ALL assigned users (regardless of payment status)
    // This ensures when admin assigns a user to a fee collector:
    //   - Umer gets user assigned → Umer's dashboard shows totalUsers = 1 immediately
    //   - MohdAli's dashboard shows totalUsers = 0 (only counts his assigned users)
    const totalUsers = await usersCollection.countDocuments(userFilter);
    console.log(`📊 Total users (assigned to ${feeCollector || assignTo || 'all'}): ${totalUsers}`);
    
    // CRITICAL: Month-level counting - count users with AT LEAST ONE paid month
    // IMPORTANT: For paid users, we check receivedBy in vouchers directly (same as paid-users endpoint)
    // For unpaid users, we pre-filter by user.feeCollector FIRST (same as unpaid-users endpoint)
    // Only pre-filter for technician (assignTo) since that's a user-level field
    let vouchersForStats = await vouchersCol.find({}).toArray();
    if (assignTo && filteredUserIds && filteredUserIds.length > 0) {
      // For technician, pre-filter by assignTo
      vouchersForStats = vouchersForStats.filter(v => 
        v.userId && filteredUserIds.includes(v.userId.toString())
      );
      console.log(`📊 Filtered vouchers by assignTo: ${vouchersForStats.length} vouchers for ${filteredUserIds.length} users`);
    } else if (feeCollector) {
      // For fee collector paid users, don't pre-filter - check receivedBy in vouchers directly
      console.log(`📊 Fee collector filter: Will check receivedBy in all vouchers for paid users (not pre-filtering by user.feeCollector)`);
    }
    
    const userIdsWithPaidMonths = new Set();
    const feeCollectorTrimmedForStats = feeCollector ? feeCollector.trim() : null;
    
    // FALLBACK: Get users assigned to this feeCollector for old payments
    let usersAssignedToFeeCollector = new Set();
    if (feeCollectorTrimmedForStats) {
      const assignedUsers = await usersCollection.find({
        feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmedForStats.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }
      }).toArray();
      assignedUsers.forEach(u => usersAssignedToFeeCollector.add(u._id.toString()));
      console.log(`📋 Dashboard Stats - Found ${usersAssignedToFeeCollector.size} users assigned to ${feeCollectorTrimmedForStats}`);
    }
    
    // Process vouchers for PAID months (check receivedBy for fee collectors)
    vouchersForStats.forEach(voucher => {
      if (Array.isArray(voucher.months)) {
        // Check if voucher has paid months
        // If feeCollector filter is provided, also check receivedBy
        const hasPaidMonth = voucher.months.some(m => {
          const isPaid = m.status === 'paid' || (m.status === 'partial' && m.paidAmount > 0);
          if (!isPaid) return false;
          
          // If feeCollector filter is provided, check receivedBy
          if (feeCollectorTrimmedForStats) {
            // Check month-level receivedBy
            const monthReceivedBy = m.receivedBy || '';
            // Also check paymentHistory for receivedBy
            const paymentHistoryReceivedBy = Array.isArray(m.paymentHistory) 
              ? m.paymentHistory.map((p) => p.receivedBy || '').filter(Boolean)
              : [];
            
            // Match if receivedBy matches feeCollector (case-insensitive)
            const monthMatches = monthReceivedBy && 
              new RegExp(`^${feeCollectorTrimmedForStats.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
            const historyMatches = paymentHistoryReceivedBy.some((rb) => 
              new RegExp(`^${feeCollectorTrimmedForStats.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
            );
            
            // FALLBACK: For old payments where receivedBy is empty or "Myself" or "Admin"
            // Check if this user is assigned to the feeCollector
            const hasReceivedBy = monthReceivedBy && monthReceivedBy !== '';
            const receivedByIsOldValue = monthReceivedBy === 'Myself' || monthReceivedBy === 'Admin';
            
            if (!hasReceivedBy || receivedByIsOldValue) {
              // No receivedBy or old value - fall back to checking if user is assigned to feeCollector
              const userIsAssigned = usersAssignedToFeeCollector.has(voucher.userId.toString());
              return userIsAssigned;
            }
            
            return monthMatches || historyMatches;
          }
          
          return true; // No feeCollector filter, include all paid months
        });
        
        if (hasPaidMonth && voucher.userId) {
          userIdsWithPaidMonths.add(voucher.userId.toString());
        }
      }
    });
    
    console.log(`📊 Dashboard Stats - Found ${userIdsWithPaidMonths.size} users with paid months${feeCollectorTrimmedForStats ? ` (filtered by receivedBy: ${feeCollectorTrimmedForStats})` : ''}`);
    
    // Count paid users (users with at least one paid month) - exclude inactive
    // CRITICAL: Only count users who actually paid to THIS fee collector (based on receivedBy)
    const paidUserIds = Array.from(userIdsWithPaidMonths).map(id => {
      try {
        return new ObjectId(id);
      } catch (e) {
        return id;
      }
    });
    
    // Build paid users query with filter
    // CRITICAL: Use receivedBy-based filtering for both fee collector and admin
    // This ensures paid users count matches what's shown in paid users list
    let paidUsersQuery = {
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ],
      status: { $in: ['paid', 'partial'] } // Only count paid/partial users
    };
    
    // Use userIds calculated from vouchers (receivedBy filter already applied)
    // CRITICAL: Also filter by expiry date - only count users whose expiry has passed (after 12 PM)
    let paidUsers = 0;
    if (paidUserIds.length > 0) {
      paidUsersQuery._id = { $in: paidUserIds };
      console.log(`🔒 Dashboard Stats - Using userIds from vouchers (receivedBy filter): ${paidUserIds.length} users`);
      
      if (assignTo) {
        paidUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
      }
      
      // Get all users matching the basic query
      const allPaidUsers = await usersCollection.find(paidUsersQuery).toArray();
      
      // Filter by expiry date - only count if expiry has passed (after 12 PM on expiry date)
      const now = new Date();
      const paidUsersWithExpiredDate = allPaidUsers.filter(user => {
        if (!user.expiryDate) return true; // No expiry date, include it
        
        try {
          let expiryDate;
          const expiry = user.expiryDate;
          
          if (expiry instanceof Date) {
            expiryDate = new Date(expiry);
          } else if (typeof expiry === 'string') {
            // Try different formats
            if (/^\d{4}-\d{2}-\d{2}/.test(expiry)) {
              expiryDate = new Date(expiry);
            } else {
              const parts = expiry.split(/[-\/]/);
              if (parts.length >= 3) {
                const [a, b, c] = parts;
                if (a.length === 4) {
                  expiryDate = new Date(a, parseInt(b) - 1, parseInt(c));
                } else {
                  expiryDate = new Date(c, parseInt(b) - 1, parseInt(a));
                }
              }
            }
          }
          
          if (!expiryDate || isNaN(expiryDate.getTime())) return true;
          
          // Set to 12 PM on expiry date
          expiryDate.setHours(12, 0, 0, 0);
          return expiryDate <= now; // Only count if expiry passed
        } catch (error) {
          return true; // Error, include it
        }
      });
      
      paidUsers = paidUsersWithExpiredDate.length;
      console.log(`✅ Paid users count: ${paidUsers} (from ${paidUserIds.length} userIds, ${allPaidUsers.length} before expiry filter)`);
    } else {
      // CRITICAL: If no userIds found (no payments to this fee collector), return 0
      // Don't query all users, as that would count users paid to OTHER fee collectors
      console.log(`📊 No paid users found for ${feeCollector || assignTo || 'this filter'}`);
      paidUsers = 0;
    }
    
    console.log(`📊 FINAL - Paid users for ${feeCollector || assignTo || 'all'}: ${paidUsers} (filtered by expiry date)`);
    
    // Count unpaid users (users with at least one unpaid month) - exclude inactive
    // CRITICAL: For fee collector, use EXACT same logic as /api/users/unpaid endpoint
    // Pre-filter users by user.feeCollector FIRST, then check their vouchers for unpaid months
    let unpaidUsersFilteredByIds = null;
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      const matchingUsers = await usersCollection.find({
        feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') }
      }).project({ _id: 1 }).toArray();
      unpaidUsersFilteredByIds = new Set(matchingUsers.map(u => u._id.toString()));
      console.log(`📊 Pre-filtered users by user.feeCollector for unpaid users: ${unpaidUsersFilteredByIds.size} users`);
    }
    
    // Filter vouchers by these pre-filtered user IDs
    let vouchersForUnpaidStats = await vouchersCol.find({}).toArray();
    if (unpaidUsersFilteredByIds && unpaidUsersFilteredByIds.size > 0) {
      vouchersForUnpaidStats = vouchersForUnpaidStats.filter(v =>
        v.userId && unpaidUsersFilteredByIds.has(v.userId.toString())
      );
      console.log(`📊 Pre-filtered vouchers by user.feeCollector for unpaid users: ${vouchersForUnpaidStats.length} vouchers for ${unpaidUsersFilteredByIds.size} users`);
    } else if (feeCollector) {
      // If feeCollector is present but no matching users, then no unpaid users
      console.log(`⚠️ No users found matching feeCollector for unpaid stats - returning zero`);
      const unpaidUsers = 0;
    } else if (assignTo && filteredUserIds && filteredUserIds.length > 0) {
      // For technician, use pre-filtered vouchers
      vouchersForUnpaidStats = vouchersForUnpaidStats.filter(v =>
        v.userId && filteredUserIds.includes(v.userId.toString())
      );
      console.log(`📊 Pre-filtered vouchers by assignTo for unpaid users: ${vouchersForUnpaidStats.length} vouchers`);
    }
    
    // Now check vouchers for unpaid AND partial months
    // CRITICAL: Count users with EITHER unpaid OR partial months (or both)
    // IMPORTANT: Only include users whose expiry date has passed (after 12 PM on expiry date)
    // This matches what user wants: unpaid + partial = total unpaid count in dashboard
    const userIdsWithUnpaidMonths = new Set();
    const userIdsWithPartialMonths = new Set();
    
    // Helper function to check if expiry date has passed
    const hasExpiryPassed = (expiryDate) => {
      if (!expiryDate) return true; // No expiry date, include it
      
      try {
        const now = new Date();
        let expiry;
        
        if (expiryDate instanceof Date) {
          expiry = new Date(expiryDate);
        } else if (typeof expiryDate === 'string') {
          // Try different formats
          if (/^\d{4}-\d{2}-\d{2}/.test(expiryDate)) {
            expiry = new Date(expiryDate);
          } else {
            const parts = expiryDate.split(/[-\/]/);
            if (parts.length >= 3) {
              const [a, b, c] = parts;
              if (a.length === 4) {
                expiry = new Date(a, parseInt(b) - 1, parseInt(c));
              } else {
                expiry = new Date(c, parseInt(b) - 1, parseInt(a));
              }
            }
          }
        }
        
        if (!expiry || isNaN(expiry.getTime())) return true;
        
        // Set to 12 PM on expiry date
        expiry.setHours(12, 0, 0, 0);
        return expiry <= now;
      } catch (error) {
        return true; // Error, include it
      }
    };
    
    vouchersForUnpaidStats.forEach(voucher => {
      if (Array.isArray(voucher.months)) {
        // CRITICAL: Only process if expiry date has passed
        if (!hasExpiryPassed(voucher.expiryDate)) {
          return; // Skip this voucher - not expired yet
        }
        
        // Check for unpaid months
        const hasUnpaidMonth = voucher.months.some(m => 
          m.status === 'unpaid' && !m.refundDate && !m.refundedAmount
        );
        
        // Check for partial months (with remaining amount > 0)
        const hasPartialMonth = voucher.months.some(m =>
          m.status === 'partial' && !m.refundDate && !m.refundedAmount && (m.remainingAmount || 0) > 0
        );
        
        if (voucher.userId) {
          const userIdStr = voucher.userId.toString();
          if (hasUnpaidMonth) {
            userIdsWithUnpaidMonths.add(userIdStr);
          }
          if (hasPartialMonth) {
            userIdsWithPartialMonths.add(userIdStr);
          }
        }
      }
    });
    
    // Combine both sets: users with unpaid OR partial months
    const allUnpaidAndPartialUserIds = new Set([...userIdsWithUnpaidMonths, ...userIdsWithPartialMonths]);
    
    console.log(`📊 Unpaid users calculation (unpaid + partial, after expiry date filter):`);
    console.log(`   - Users with unpaid months: ${userIdsWithUnpaidMonths.size}`);
    console.log(`   - Users with partial months: ${userIdsWithPartialMonths.size}`);
    console.log(`   - Total (unpaid + partial): ${allUnpaidAndPartialUserIds.size}`);
    console.log(`   - Note: Excludes users whose expiry date hasn't been reached (before 12 PM on expiry date)`);
    
    const unpaidUserIdsArray = Array.from(allUnpaidAndPartialUserIds);
    
    // CRITICAL: For fee collector, ensure unpaidUserIds only includes users that match feeCollector
    let finalUnpaidUserIds = unpaidUserIdsArray;
    if (feeCollector && unpaidUsersFilteredByIds && unpaidUsersFilteredByIds.size > 0) {
      finalUnpaidUserIds = unpaidUserIdsArray.filter(id =>
        unpaidUsersFilteredByIds.has(id.toString())
      );
      console.log(`🔒 Filtered unpaidUserIds by feeCollector: ${unpaidUserIdsArray.length} → ${finalUnpaidUserIds.length}`);
    }
    
    const unpaidUserIds = finalUnpaidUserIds.map(id => {
      try {
        return new ObjectId(id);
      } catch (e) {
        return id;
      }
    });
    
    // Build unpaid users query with filter
    let unpaidUsersQuery = {
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    if (unpaidUserIds.length > 0) {
      unpaidUsersQuery._id = { $in: unpaidUserIds };
    }
    // Removed feeCollector filter here to avoid double filtering, as it's handled by pre-filtering
    if (assignTo) {
      unpaidUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    
    // CRITICAL: Use voucher-based unpaid count (includes expiry filter)
    // This ensures users only show as unpaid AFTER their expiry date passes (12 PM)
    // 
    // Example scenarios:
    //   - User created today with expiry tomorrow → unpaidUsers = 0 (expiry not passed) ✓
    //   - User expired yesterday → unpaidUsers = 1 (expiry passed) ✓
    //   - Pay Later user created today → unpaidUsers = 0 (not expired yet) ✓
    //
    // This prevents newly created Pay Later users from showing in unpaid count immediately
    const unpaidUsers = unpaidUserIds.length > 0 ? await usersCollection.countDocuments(unpaidUsersQuery) : 0;
    
    console.log(`📊 Dashboard Stats - Unpaid users calculation:`);
    console.log(`   - totalUsers: ${totalUsers}, paidUsers: ${paidUsers}, unpaidUsers: ${unpaidUsers}`);
    console.log(`   - Unpaid users filtered by expiry date (only those past 12 PM on expiry date)`);
    console.log(`   - initialVouchers: ${await vouchersCol.countDocuments({})}`);
    console.log(`   - vouchersAfterAssignToFilter: ${assignTo ? vouchersForStats.length : 'N/A'}`);
    console.log(`   - vouchersAfterFeeCollectorPreFilter: ${feeCollector ? (unpaidUsersFilteredByIds ? vouchersForUnpaidStats.length : 'N/A') : 'N/A'}`);
    console.log(`   - userIdsWithUnpaidMonthsSize: ${userIdsWithUnpaidMonths.size}`);
    console.log(`   - finalUnpaidUserIdsLength: ${finalUnpaidUserIds.length}`);
    
    // Expiring soon (TOMORROW) - include ALL users expiring tomorrow
    // CRITICAL: This is a REMINDER list, not a payment status list
    // Show all users (paid, unpaid, partial, pending, superbalance) expiring tomorrow
    // Stats counts are filtered by expiry date separately above
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const endOfTomorrow = new Date(tomorrow);
    endOfTomorrow.setHours(23, 59, 59, 999);
    
    // Build expiring soon query with filter
    let expiringSoonQuery = {
      status: { $in: ['paid', 'partial', 'unpaid', 'pending', 'superbalance'] },
      expiryDate: { 
        $gte: tomorrow.toISOString(), 
        $lte: endOfTomorrow.toISOString() 
      },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    if (feeCollector) {
      expiringSoonQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    if (assignTo) {
      expiringSoonQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    
    const expiringSoon = await usersCollection.countDocuments(expiringSoonQuery);
    
    // Active users - users with serviceStatus = 'active' or not set (default active)
    let activeUsersQuery = {
      $or: [
        { serviceStatus: 'active' },
        { serviceStatus: { $exists: false } },
        { serviceStatus: null }
      ]
    };
    if (assignTo) {
      activeUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    if (feeCollector) {
      activeUsersQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    const activeUsers = await usersCollection.countDocuments(activeUsersQuery);
    
    // Deactivated/Inactive users - users with serviceStatus = 'inactive'
    let deactivatedQuery = {
      serviceStatus: 'inactive'
    };
    if (assignTo) {
      deactivatedQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    if (feeCollector) {
      deactivatedQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    const deactivatedUsers = await usersCollection.countDocuments(deactivatedQuery);
    
    console.log(`📊 User counts - Active: ${activeUsers}, Inactive: ${deactivatedUsers}, Total: ${totalUsers}`);
    
    // Total income - CRITICAL: Check incomes collection first, only recalculate if needed
    // This ensures transfers are not lost on dashboard refresh
    let totalIncome = 0;
    let cashIncome = 0;
    let bankIncome = 0;
    const feeCollectorTrimmed = feeCollector ? feeCollector.trim() : null;
    
    // CHECK INCOMES COLLECTION FIRST
    const incomesCol = db.collection('incomes');
    let shouldRecalculate = false;
    
    if (feeCollectorTrimmed) {
      // Check if fee collector has existing income in database
      const existingIncome = await incomesCol.findOne({ 
        name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } 
      });
      
      if (existingIncome) {
        // Use existing income from database (don't recalculate)
        // CRITICAL: Even if cashIncome is 0, use it! (could be 0 after transfer)
        cashIncome = existingIncome.cashIncome || 0;
        bankIncome = existingIncome.bankIncome || 0;
        totalIncome = cashIncome + bankIncome;
        console.log(`💰 Using existing income from database for ${feeCollectorTrimmed}: Cash Rs ${cashIncome}, Bank Rs ${bankIncome} (preserving transfers)`);
      } else {
        // No income record found, recalculate from vouchers
        shouldRecalculate = true;
        console.log(`💰 No existing income record found, calculating from vouchers for ${feeCollectorTrimmed}`);
      }
    } else {
      // Admin - check existing income
      const existingAdminIncome = await incomesCol.findOne({ 
        name: { $regex: new RegExp(`^Admin$`, 'i') } 
      });
      
      if (existingAdminIncome) {
        // Use existing income from database
        // CRITICAL: Even if cashIncome is 0, use it! (could be 0 after expenses/transfers)
        cashIncome = existingAdminIncome.cashIncome || 0;
        bankIncome = existingAdminIncome.bankIncome || 0;
        totalIncome = cashIncome + bankIncome;
        console.log(`💰 Using existing income from database for Admin: Cash Rs ${cashIncome}, Bank Rs ${bankIncome} (preserving transfers/expenses)`);
      } else {
        // No income record found, recalculate from vouchers
        shouldRecalculate = true;
        console.log(`💰 No existing income found, calculating from vouchers for Admin`);
      }
    }
    
    // RECALCULATE FROM VOUCHERS ONLY IF NEEDED
    if (shouldRecalculate && feeCollectorTrimmed) {
      // Calculate income from vouchers where receivedBy matches feeCollector
      console.log(`💰 Calculating income from vouchers with receivedBy: ${feeCollectorTrimmed}`);
      
      const allVouchersForIncome = await vouchersCol.find({}).toArray();
      let incomeFromVouchers = 0;
      
      allVouchersForIncome.forEach(voucher => {
        if (Array.isArray(voucher.months)) {
          voucher.months.forEach(month => {
            // Check if month has paid amount and receivedBy matches
            const monthReceivedBy = month.receivedBy || '';
            const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];
            const paymentHistoryReceivedBy = paymentHistory.map((p) => p.receivedBy || '').filter(Boolean);
            
            const monthMatchesReceivedBy = monthReceivedBy && 
              new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
            const historyMatchesReceivedBy = paymentHistoryReceivedBy.some((rb) => 
              new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
            );
            
            if (monthMatchesReceivedBy || historyMatchesReceivedBy) {
              // If paymentHistory exists, use it (more accurate - individual payments)
              // Otherwise use month.paidAmount
              if (paymentHistory.length > 0) {
                paymentHistory.forEach((payment) => {
                  const paymentReceivedBy = payment.receivedBy || '';
                  if (paymentReceivedBy && new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(paymentReceivedBy)) {
                    const amount = Number(payment.amount || 0);
                    incomeFromVouchers += amount;
                    
                    // Separate by payment method
                    const paymentMethodStr = (payment.paymentMethod || '').trim().toLowerCase();
                    if (paymentMethodStr === 'cash') {
                      cashIncome += amount;
                    } else if (paymentMethodStr === 'bank transfer') {
                      bankIncome += amount;
                    }
    }
                });
              } else if (monthMatchesReceivedBy) {
                // No paymentHistory, but month.receivedBy matches - use month.paidAmount
                const paidAmt = Number(month.paidAmount || 0);
                incomeFromVouchers += paidAmt;
                // If no paymentHistory, check month.paymentMethod
                const monthMethod = (month.paymentMethod || '').trim().toLowerCase();
                if (monthMethod === 'cash') {
                  cashIncome += paidAmt;
                } else if (monthMethod === 'bank transfer') {
                  bankIncome += paidAmt;
                }
              }
            }
          });
        }
      });
      
      totalIncome = incomeFromVouchers;
      console.log(`💰 Total income from vouchers (receivedBy=${feeCollectorTrimmed}): Rs ${totalIncome}`);
      console.log(`💵 Cash income: Rs ${cashIncome}`);
      console.log(`🏦 Bank income: Rs ${bankIncome}`);
    } else if (shouldRecalculate && !feeCollectorTrimmed) {
      // No feeCollector filter - Admin login - Recalculate from vouchers
      // CRITICAL: Admin ki income sirf "Admin" ya "Myself" select karne par increase hogi
      // Employee name select karne par admin ki income increase nahi hogi
      console.log(`💰 Admin login - Calculating income from vouchers with receivedBy: "Admin" or "Myself"`);
      
      const allVouchersForIncome = await vouchersCol.find({}).toArray();
      let incomeFromVouchers = 0;
      
      allVouchersForIncome.forEach(voucher => {
        if (Array.isArray(voucher.months)) {
          voucher.months.forEach(month => {
            // Check if month has paid amount and receivedBy is "Admin" or "Myself"
            const monthReceivedBy = month.receivedBy || '';
            const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];
            
            console.log(`\n🔍 Checking month: ${month.month}`);
            console.log(`   month.paidAmount: Rs ${month.paidAmount || 0}`);
            console.log(`   month.receivedBy: ${monthReceivedBy}`);
            console.log(`   paymentHistory.length: ${paymentHistory.length}`);
            if (paymentHistory.length > 0) {
              console.log(`   paymentHistory entries:`, paymentHistory.map(p => `Rs ${p.amount} by ${p.receivedBy} (${p.paymentMethod})`).join(', '));
    }
    
            // CRITICAL: Always use paymentHistory for accurate income calculation
            // paymentHistory contains individual payments with their receivedBy values
            // This prevents double-counting and ensures only payments made by "Admin" or "Myself" are counted
            // IMPORTANT: We ONLY count from paymentHistory to avoid counting total paidAmount
            // which might include payments from other receivers (e.g., employee's 1000 + admin's 200 = 1200)
            if (paymentHistory.length > 0) {
              // Use paymentHistory - only count payments where receivedBy is "Admin" or "Myself"
              // This is the most accurate method as it tracks each individual payment
              let monthIncome = 0;
              paymentHistory.forEach((payment) => {
                const paymentReceivedBy = payment.receivedBy || '';
                const paymentAmount = Number(payment.amount || 0);
                const paymentMethodStr = (payment.paymentMethod || '').trim().toLowerCase();
                
                // Check for both "Admin" and "Myself"
                const isAdminPayment = paymentReceivedBy && 
                  (new RegExp(`^Admin$`, 'i').test(paymentReceivedBy.trim()) || 
                   new RegExp(`^Myself$`, 'i').test(paymentReceivedBy.trim()));
                
                if (isAdminPayment) {
                  monthIncome += paymentAmount;
                  
                  // Separate by payment method
                  if (paymentMethodStr === 'cash') {
                    cashIncome += paymentAmount;
                  } else if (paymentMethodStr === 'bank transfer') {
                    bankIncome += paymentAmount;
                  }
                  
                  console.log(`   ✅ Counting payment from paymentHistory: Rs ${paymentAmount} (receivedBy: ${paymentReceivedBy}, method: ${payment.paymentMethod})`);
                } else {
                  console.log(`   ⏭️ Skipping payment: Rs ${paymentAmount} (receivedBy: ${paymentReceivedBy}, not "Admin" or "Myself")`);
    }
              });
              incomeFromVouchers += monthIncome;
              console.log(`   💰 Month income added: Rs ${monthIncome}`);
            } else {
              // No paymentHistory - DO NOT COUNT to avoid double-counting
              // month.paidAmount includes ALL payments (employee + admin), so we can't determine
              // how much was paid by admin without paymentHistory
              // This ensures accurate income calculation
              console.log(`   ⚠️ No paymentHistory for ${month.month} - skipping (cannot determine admin's portion without paymentHistory)`);
              console.log(`   ⚠️ month.paidAmount = Rs ${month.paidAmount || 0} (may include payments from other receivers)`);
              console.log(`   ⏭️ NOT counting this month (no paymentHistory)`);
            }
          });
        }
      });
      
      totalIncome = incomeFromVouchers;
      console.log(`💰 Admin total income from vouchers (receivedBy="Admin" or "Myself"): Rs ${totalIncome}`);
      console.log(`💵 Cash income: Rs ${cashIncome}`);
      console.log(`🏦 Bank income: Rs ${bankIncome}`);
      
      // CRITICAL: Add transfer amounts from fee collectors to admin income
      // When fee collectors transfer money to admin, it should be added to admin's total income
      try {
        let collectionsCollection = db.collection('collections');
        
        // Check if collections collection exists
        const collections = await db.listCollections().toArray();
        const collectionExists = collections.some(c => c.name === 'collections');
        
        if (collectionExists) {
          // Get all transfer amounts (fee collectors se admin ko transfer kiye gaye amounts)
          const allTransfers = await collectionsCollection.find({}).toArray();
          let totalTransferAmount = 0;
          
          allTransfers.forEach((transfer) => {
            const transferAmount = Number(transfer.amount || 0);
            totalTransferAmount += transferAmount;
            console.log(`   💰 Transfer from ${transfer.feeCollector}: Rs ${transferAmount}`);
          });
          
          totalIncome += totalTransferAmount;
          console.log(`💰 Total transfer amount added to admin income: Rs ${totalTransferAmount}`);
          console.log(`💰 Admin total income (vouchers + transfers): Rs ${totalIncome}`);
        } else {
          console.log(`ℹ️ Collections collection does not exist - no transfer amounts to add`);
        }
      } catch (error) {
        console.error('❌ Error fetching transfer amounts:', error);
        // Don't fail the request if transfer fetch fails, just log the error
      }
    }
    
    // Total expense - combine both transactions and expenses collections
    // For fee collector, filter by paidBy field
    const expenseMatchQuery = { type: 'expense' };
    if (feeCollector) {
      expenseMatchQuery.paidBy = { $regex: new RegExp(`^${feeCollector.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
    }
    
    const expenseResultFromTransactions = await transactionsCollection.aggregate([
      { $match: expenseMatchQuery },
      { $group: { _id: null, total: { $sum: '$amount' } } }
    ]).toArray();
    
    // Get expenses from dedicated expenses collection
    const expensesCollection = db.collection('expenses');
    const expensesMatchQuery = {};
    if (feeCollector) {
      expensesMatchQuery.paidBy = { $regex: new RegExp(`^${feeCollector.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
    }
    
    const expenseResultFromExpenses = await expensesCollection.aggregate([
      { $match: expensesMatchQuery },
      { $group: { _id: null, total: { $sum: '$amount' } } }
    ]).toArray();
    
    const transactionsExpenseTotal = expenseResultFromTransactions.length > 0 ? expenseResultFromTransactions[0].total : 0;
    const expensesTotal = expenseResultFromExpenses.length > 0 ? expenseResultFromExpenses[0].total : 0;
    
    // Add both totals for the complete expense amount
    const totalExpense = Number(transactionsExpenseTotal) + Number(expensesTotal);
    console.log(`💰 Dashboard stats: Total expense = ${totalExpense} (transactions: ${transactionsExpenseTotal}, expenses: ${expensesTotal})${feeCollector ? ` (filtered by paidBy: ${feeCollector})` : ''}`);
    
    // Outstanding/Balance - Calculate from vouchers to match unpaid-users.tsx display amounts
    const vouchersCollection = db.collection('vouchers');
    const allVouchers = await vouchersCollection.find({}).toArray();
    
    // Get unpaid and partial users (same as unpaid-users.tsx)
    let unpaidUsersListQuery = {
      status: 'unpaid',
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    if (feeCollector) {
      unpaidUsersListQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    if (assignTo) {
      unpaidUsersListQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    
    const unpaidUsersList = await usersCollection.find(unpaidUsersListQuery).toArray();
    
    let partialUsersQuery = {
      status: 'partial',
      remainingAmount: { $gt: 0 },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    if (feeCollector) {
      partialUsersQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    if (assignTo) {
      partialUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }
    
    const partialUsers = await usersCollection.find(partialUsersQuery).toArray();
    
    // Calculate outstanding using voucher-based totals (same as unpaid-users.tsx)
    const calculateUserOutstanding = (user) => {
      const userVoucher = allVouchers.find(v => v.userId === user._id.toString());
      if (userVoucher && Array.isArray(userVoucher.months)) {
        return userVoucher.months.reduce((sum, month) => {
          const rem = (month.remainingAmount !== undefined && month.remainingAmount !== null)
            ? Number(month.remainingAmount)
            : Math.max(0, Number(month.packageFee || 0) - Number(month.paidAmount || 0));
          return rem > 0 ? sum + rem : sum;
        }, 0);
      }
      return 0; // No voucher found
    };
    
    const unpaidTotal = unpaidUsersList.reduce((sum, user) => sum + calculateUserOutstanding(user), 0);
    const partialTotal = partialUsers.reduce((sum, user) => sum + calculateUserOutstanding(user), 0);
    
    console.log('📊 Dashboard Outstanding Calculation:', {
      unpaidUsersCount: unpaidUsersList.length,
      partialUsersCount: partialUsers.length,
      unpaidTotal,
      partialTotal,
      totalOutstanding: unpaidTotal + partialTotal
    });
    
    const outstanding = Number(unpaidTotal) + Number(partialTotal); // Total from voucher-based calculation
    const balanceCustomers = partialUsers.length;
    
    // 💰 SAVE/UPDATE INCOME IN incomes COLLECTION
    // ONLY save if we recalculated from vouchers (shouldRecalculate = true)
    // If we used existing income, don't overwrite it (to preserve transfers)
    try {
      if (shouldRecalculate && feeCollectorTrimmed) {
        // Fee Collector ki income save/update karein
        const existingIncome = await incomesCollection.findOne({ 
          name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } 
        });
        
        if (existingIncome) {
          // Update existing income - cashIncome and bankIncome
          await incomesCollection.updateOne(
            { name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
            { 
              $set: { 
                cashIncome: cashIncome || 0,
                bankIncome: bankIncome || 0,
                lastUpdated: new Date()
              } 
            }
          );
          console.log(`💰 Updated income for ${feeCollectorTrimmed}: Cash Rs ${cashIncome}, Bank Rs ${bankIncome}`);
        } else {
          // Create new income record - cashIncome and bankIncome
          await incomesCollection.insertOne({
            name: feeCollectorTrimmed,
            cashIncome: cashIncome || 0,
            bankIncome: bankIncome || 0,
            createdAt: new Date(),
            lastUpdated: new Date()
          });
          console.log(`💰 Created new income record for ${feeCollectorTrimmed}: Cash Rs ${cashIncome}, Bank Rs ${bankIncome}`);
        }
      } else if (shouldRecalculate && !feeCollectorTrimmed) {
        // Admin ki income save/update karein (only if recalculated)
        const existingAdminIncome = await incomesCollection.findOne({ 
          name: { $regex: new RegExp(`^Admin$`, 'i') } 
        });
        
        if (existingAdminIncome) {
          // Update existing admin income - cashIncome and bankIncome
          await incomesCollection.updateOne(
            { name: { $regex: new RegExp(`^Admin$`, 'i') } },
            { 
              $set: { 
                cashIncome: cashIncome || 0,
                bankIncome: bankIncome || 0,
                lastUpdated: new Date()
              } 
            }
          );
          console.log(`💰 Updated income for Admin: Cash Rs ${cashIncome}, Bank Rs ${bankIncome}`);
        } else {
          // Create new admin income record - cashIncome and bankIncome
          await incomesCollection.insertOne({
            name: 'Admin',
            cashIncome: cashIncome || 0,
            bankIncome: bankIncome || 0,
            createdAt: new Date(),
            lastUpdated: new Date()
          });
          console.log(`💰 Created new income record for Admin: Cash Rs ${cashIncome}, Bank Rs ${bankIncome}`);
        }
      } else {
        // Using existing income, not overwriting
        console.log(`💰 Skipping income save - using existing income from database (preserves transfers)`);
      }
    } catch (incomeError) {
      console.error('❌ Error saving income to incomes collection:', incomeError);
      // Don't fail the request if income save fails, just log the error
    }
    
    res.status(200).json({
      success: true,
      data: {
        totalUsers,
        paidUsers,
        totalIncome,
        cashIncome: cashIncome || 0,
        bankIncome: bankIncome || 0,
        totalExpense,
        unpaidUsers,
        outstanding,
        balance: outstanding, // Same as outstanding - sum of remainingAmount from partial users
        balanceCustomers, // Number of customers with remaining balance
        expiringSoon,
        activeUsers,        // Users with serviceStatus = 'active' or not set
        deactivatedUsers    // Users with serviceStatus = 'inactive'
      }
    });
  } catch (error) {
    console.error('Error fetching dashboard stats:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching dashboard stats',
      error: error.message
    });
  }
});

// GET paid users (with date filter and pagination)
app.get('/api/users/paid', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const paymentDate = req.query.paymentDate; // YYYY-MM-DD format (deprecated, use fromDate/toDate)
    const fromDate = req.query.fromDate; // YYYY-MM-DD format
    const toDate = req.query.toDate; // YYYY-MM-DD format
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    
    let userIds = [];
    let totalCollectionAmount = 0; // Track total collection for date range
    
    // If date range filter is provided (fromDate and toDate)
    if (fromDate && toDate) {
      const [fromYearStr, fromMonthStr, fromDayStr] = fromDate.split('-');
      const [toYearStr, toMonthStr, toDayStr] = toDate.split('-');
      
      const fromYear = parseInt(fromYearStr, 10);
      const fromMonth = parseInt(fromMonthStr, 10) - 1;
      const fromDay = parseInt(fromDayStr, 10);
      
      const toYear = parseInt(toYearStr, 10);
      const toMonth = parseInt(toMonthStr, 10) - 1;
      const toDay = parseInt(toDayStr, 10);
      
      if (!Number.isNaN(fromYear) && !Number.isNaN(fromMonth) && !Number.isNaN(fromDay) &&
          !Number.isNaN(toYear) && !Number.isNaN(toMonth) && !Number.isNaN(toDay)) {
        
        // CRITICAL: Use PKT (UTC+5) timezone for date comparison
        // Create date strings in YYYY-MM-DD format for simple string comparison
        const startDateStr = `${fromYear}-${String(fromMonth + 1).padStart(2, '0')}-${String(fromDay).padStart(2, '0')}`;
        const endDateStr = `${toYear}-${String(toMonth + 1).padStart(2, '0')}-${String(toDay).padStart(2, '0')}`;

        console.log(`📅 Date range filter: ${startDateStr} to ${endDateStr} (using PKT string comparison)`);

        const formatToIso = (date) => date.toISOString().split('T')[0];
        const formatToLocal = (date) => {
          try {
            return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Karachi' }).format(date);
          } catch (err) {
            return formatToIso(date);
          }
        };

        const normalizeToIsoString = (value) => {
          if (!value) return null;
          if (value instanceof Date) {
            // Prioritize PKT timezone format
            return [formatToLocal(value)];
          }

          if (typeof value === 'string') {
            const native = new Date(value);
            if (!Number.isNaN(native.getTime())) {
              // Prioritize PKT timezone format
              return [formatToLocal(native)];
            }

            const parts = value.split(/[-\/]/);
            if (parts.length === 3) {
              let [a, b, c] = parts;
              if (a.length === 4) {
                // Already YYYY-MM-DD format
                return [`${a}-${b.padStart(2, '0')}-${c.padStart(2, '0')}`];
              }
              // DD-MM-YYYY or DD/MM/YYYY format - convert to YYYY-MM-DD
              const dayPart = a.padStart(2, '0');
              const monthPart = b.padStart(2, '0');
              const yearPart = c;
              return [`${yearPart}-${monthPart}-${dayPart}`];
            }
          }

          return null;
        };

        const isDateInRange = (value) => {
          const normalized = normalizeToIsoString(value);
          if (!normalized) return false;
          
          // Use PKT timezone string comparison (YYYY-MM-DD format)
          return normalized.some((isoDate) => {
            // isoDate is already in YYYY-MM-DD format
            // Simple string comparison works because YYYY-MM-DD sorts correctly
            return isoDate >= startDateStr && isoDate <= endDateStr;
          });
        };

        // Include both 'paid' and 'partial' months (users with payments show in Paid tab)
        const vouchers = await vouchersCollection.find({ 'months.status': { $in: ['paid', 'partial'] } }).toArray();
        const feeCollectorTrimmed = feeCollector ? feeCollector.trim() : null;

        const filteredVouchers = vouchers.filter((voucher) => {
          const months = Array.isArray(voucher.months) ? voucher.months : [];
          // Include both 'paid' and 'partial' months (show users who made payments)
          const paidOrPartialMonths = months.filter((month) => ['paid', 'partial'].includes(month.status));

          const monthMatches = paidOrPartialMonths.some((month) => {
            // Check if date is in range
            const dateInRange = isDateInRange(month.createdAt) || isDateInRange(month.date) ||
              (Array.isArray(month.paymentHistory) && month.paymentHistory.some((entry) => isDateInRange(entry?.date)));
            
            if (!dateInRange) return false;
            
            // Check receivedBy filter
            if (feeCollectorTrimmed) {
              // Fee collector filter - check if receivedBy matches feeCollector
              const monthReceivedBy = month.receivedBy || '';
              const paymentHistoryReceivedBy = Array.isArray(month.paymentHistory) 
                ? month.paymentHistory.map((p) => p.receivedBy || '').filter(Boolean)
                : [];
              
              const monthMatchesReceivedBy = monthReceivedBy && 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
              const historyMatchesReceivedBy = paymentHistoryReceivedBy.some((rb) => 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
              );
              
              return monthMatchesReceivedBy || historyMatchesReceivedBy;
            } else {
              // No fee collector filter (Admin) - show ALL paid/partial payments regardless of receivedBy
              return true;
            }
          });

          if (monthMatches) {
            // Calculate total collection from matched paid/partial months
            paidOrPartialMonths.forEach((month) => {
              const dateInRange = isDateInRange(month.createdAt) || isDateInRange(month.date) ||
                (Array.isArray(month.paymentHistory) && month.paymentHistory.some((entry) => isDateInRange(entry?.date)));
              
              if (dateInRange) {
                // Check feeCollector filter if provided
                if (feeCollectorTrimmed) {
                  const monthReceivedBy = month.receivedBy || '';
                  const paymentHistoryReceivedBy = Array.isArray(month.paymentHistory) 
                    ? month.paymentHistory.map((p) => p.receivedBy || '').filter(Boolean)
                    : [];
                  
                  const monthMatchesReceivedBy = monthReceivedBy && 
                    new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
                  const historyMatchesReceivedBy = paymentHistoryReceivedBy.some((rb) => 
                    new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
                  );
                  
                  if (monthMatchesReceivedBy || historyMatchesReceivedBy) {
                    totalCollectionAmount += (month.paidAmount || month.packageFee || 0);
                  }
                } else {
                  totalCollectionAmount += (month.paidAmount || month.packageFee || 0);
                }
              }
            });
            return true;
          }

          const topLevelMatch = (isDateInRange(voucher.createdAt) || isDateInRange(voucher.updatedAt));
          return topLevelMatch && paidOrPartialMonths.length > 0;
        });

        console.log(`📊 Query result: Found ${filteredVouchers.length} vouchers in date range ${fromDate} to ${toDate}${feeCollectorTrimmed ? ` (filtered by receivedBy: ${feeCollectorTrimmed})` : ''}`);
        console.log(`💰 Total collection amount: Rs ${totalCollectionAmount}`);
        
        userIds = filteredVouchers.map(v => v.userId);
        console.log(`Found ${userIds.length} user(s) with paid/partial activity in date range`);
      } else {
        console.log(`⚠️ Invalid date range received: ${fromDate} to ${toDate}`);
      }
    }
    // Fallback to single date filter for backward compatibility
    else if (paymentDate) {
      const [yearStr, monthStr, dayStr] = paymentDate.split('-');
      const year = parseInt(yearStr, 10);
      const month = parseInt(monthStr, 10) - 1; // JS Date month is 0-indexed
      const day = parseInt(dayStr, 10);
      
      if (!Number.isNaN(year) && !Number.isNaN(month) && !Number.isNaN(day)) {
        const startOfDay = new Date(year, month, day, 0, 0, 0, 0);
        const endOfDay = new Date(year, month, day + 1, 0, 0, 0, 0);

        console.log(`Payment date filter: ${paymentDate} → window ${startOfDay.toISOString()} to ${endOfDay.toISOString()}`);

        const formatToIso = (date) => date.toISOString().split('T')[0];
        const formatToLocal = (date) => {
          try {
            return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Karachi' }).format(date);
          } catch (err) {
            // Fallback to ISO if Intl with timezone not available
            return formatToIso(date);
          }
        };

        const normalizeToIsoString = (value) => {
          if (!value) return null;
          if (value instanceof Date) {
            return [formatToIso(value), formatToLocal(value)];
          }

          if (typeof value === 'string') {
            // handles ISO strings, yyyy-mm-dd, dd-mm-yyyy, dd/mm/yyyy
            const native = new Date(value);
            if (!Number.isNaN(native.getTime())) {
              return [formatToIso(native), formatToLocal(native)];
            }

            const parts = value.split(/[-\/]/);
            if (parts.length === 3) {
              let [a, b, c] = parts;
              if (a.length === 4) {
                // already yyyy-mm-dd style
                return [`${a}-${b.padStart(2, '0')}-${c.padStart(2, '0')}`];
              }
              // assume dd-mm-yyyy
              const dayPart = a.padStart(2, '0');
              const monthPart = b.padStart(2, '0');
              const yearPart = c;
              return [`${yearPart}-${monthPart}-${dayPart}`];
            }
          }

          return null;
        };

        const matchesPaymentDate = (value) => {
          const normalized = normalizeToIsoString(value);
          if (!normalized) return false;
          return normalized.some((iso) => iso === paymentDate);
        };

        // Include both 'paid' and 'partial' months (users with payments show in Paid tab)
        const vouchers = await vouchersCollection.find({ 'months.status': { $in: ['paid', 'partial'] } }).toArray();
        const feeCollectorTrimmed = feeCollector ? feeCollector.trim() : null;

        const filteredVouchers = vouchers.filter((voucher) => {
          const months = Array.isArray(voucher.months) ? voucher.months : [];
          // Include both 'paid' and 'partial' months (show users who made payments)
          const paidOrPartialMonths = months.filter((month) => ['paid', 'partial'].includes(month.status));

          const monthMatches = paidOrPartialMonths.some((month) => {
            // Check date match first
            const dateMatches = matchesPaymentDate(month.createdAt) || matchesPaymentDate(month.date) ||
              (Array.isArray(month.paymentHistory) && month.paymentHistory.some((entry) => matchesPaymentDate(entry?.date)));
            
            if (!dateMatches) return false;
            
            // Check receivedBy filter
            if (feeCollectorTrimmed) {
              // Fee collector filter - check if receivedBy matches feeCollector
              const monthReceivedBy = month.receivedBy || '';
              const paymentHistoryReceivedBy = Array.isArray(month.paymentHistory) 
                ? month.paymentHistory.map((p) => p.receivedBy || '').filter(Boolean)
                : [];
              
              const monthMatchesReceivedBy = monthReceivedBy && 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
              const historyMatchesReceivedBy = paymentHistoryReceivedBy.some((rb) => 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
              );
              
              return monthMatchesReceivedBy || historyMatchesReceivedBy;
            } else {
              // No fee collector filter (Admin) - show ALL paid/partial payments regardless of receivedBy
              return true;
            }
          });

          if (monthMatches) {
            return true;
          }

          const topLevelMatch = (matchesPaymentDate(voucher.createdAt) || matchesPaymentDate(voucher.updatedAt));
          return topLevelMatch && paidOrPartialMonths.length > 0;
        });

        console.log(`📊 Query result: Found ${filteredVouchers.length} vouchers matching ${paymentDate}${feeCollectorTrimmed ? ` (filtered by receivedBy: ${feeCollectorTrimmed})` : ''}`);
        filteredVouchers.forEach(v => {
          console.log(`  - User: ${v.userName}, voucherCreated: ${v.createdAt}, months: ${(v.months || []).length}`);
        });

        userIds = filteredVouchers.map(v => v.userId);
        console.log(`Found ${userIds.length} user(s) with paid/partial activity on ${paymentDate}`);
      } else {
        console.log(`⚠️ Invalid paymentDate received: ${paymentDate}`);
      }
    }
    
    // CRITICAL: Month-level filtering - show users who have AT LEAST ONE paid month
    // Check vouchers to find users with paid/partial months
    // IMPORTANT: If feeCollector filter is provided, also check receivedBy in vouchers
    let usersWithPaidMonths = [];
    
    if (!paymentDate && !fromDate && !toDate) {
      // No date filter at all - check all vouchers for paid months
      const allVouchers = await vouchersCollection.find({}).toArray();
      
      const userIdsWithPaidMonths = new Set();
      const feeCollectorTrimmed = feeCollector ? feeCollector.trim() : null;
      
      // FALLBACK: Get users assigned to this feeCollector for old payments
      let usersAssignedToFeeCollector = new Set();
      if (feeCollectorTrimmed) {
        const assignedUsers = await usersCollection.find({
          feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }
        }).toArray();
        assignedUsers.forEach(u => usersAssignedToFeeCollector.add(u._id.toString()));
        console.log(`📋 Found ${usersAssignedToFeeCollector.size} users assigned to ${feeCollectorTrimmed}`);
      }
      
      allVouchers.forEach(voucher => {
        if (Array.isArray(voucher.months)) {
          // Check if voucher has paid or partial months (users who made payments)
          // If feeCollector filter is provided, also check receivedBy
          const hasPaidMonth = voucher.months.some(m => {
            // Include both 'paid' and 'partial' status (show users who made payments)
            const isPaid = m.status === 'paid' || m.status === 'partial';
            if (!isPaid) return false;
            
            // Check receivedBy filter
            if (feeCollectorTrimmed) {
              // Fee collector filter - check if receivedBy matches feeCollector
              const monthReceivedBy = m.receivedBy || '';
              const paymentHistoryReceivedBy = Array.isArray(m.paymentHistory) 
                ? m.paymentHistory.map((p) => p.receivedBy || '').filter(Boolean)
                : [];
              
              // Match if receivedBy matches feeCollector (case-insensitive)
              const monthMatches = monthReceivedBy && 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
              const historyMatches = paymentHistoryReceivedBy.some((rb) => 
                new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)
              );
              
              // FALLBACK: For old payments where receivedBy is empty or "Myself" or "Admin"
              // Check if this user is assigned to the feeCollector
              // This ensures old data still works while new payments use receivedBy
              const hasReceivedBy = monthReceivedBy && monthReceivedBy !== '';
              const receivedByIsOldValue = monthReceivedBy === 'Myself' || monthReceivedBy === 'Admin';
              
              if (!hasReceivedBy || receivedByIsOldValue) {
                // No receivedBy or old value - fall back to checking if user is assigned to feeCollector
                // This maintains backward compatibility with old payments
                const userIsAssigned = usersAssignedToFeeCollector.has(voucher.userId.toString());
                
                if (userIdsWithPaidMonths.size < 3) {
                  console.log(`\n🔍 Fallback check for user ${voucher.userId}:`);
                  console.log(`   receivedBy: "${monthReceivedBy}" (${!hasReceivedBy ? 'empty' : 'old value'})`);
                  console.log(`   User assigned to ${feeCollectorTrimmed}? ${userIsAssigned}`);
                }
                
                return userIsAssigned;
              }
              
              // Debug logging for first few vouchers
              if (userIdsWithPaidMonths.size < 3) {
                console.log(`\n🔍 Checking voucher for user ${voucher.userId}:`);
                console.log(`   Month: ${m.month}, Status: ${m.status}, Paid: ${m.paidAmount}`);
                console.log(`   month.receivedBy: "${monthReceivedBy}"`);
                console.log(`   paymentHistory: ${paymentHistoryReceivedBy.length > 0 ? paymentHistoryReceivedBy.join(', ') : 'None'}`);
                console.log(`   Looking for: "${feeCollectorTrimmed}"`);
                console.log(`   monthMatches: ${monthMatches}, historyMatches: ${historyMatches}`);
              }
              
              return monthMatches || historyMatches;
            } else {
              // No fee collector filter (Admin) - show ALL paid/partial payments regardless of receivedBy
              return true;
            }
          });
          
          if (hasPaidMonth && voucher.userId) {
            userIdsWithPaidMonths.add(voucher.userId.toString());
          }
        }
      });
      
      usersWithPaidMonths = Array.from(userIdsWithPaidMonths);
      console.log(`📊 Found ${usersWithPaidMonths.length} users with at least one paid/partial month${feeCollectorTrimmed ? ` (filtered by receivedBy: ${feeCollectorTrimmed})` : ''}`);
      
      // Debug: Log first few user IDs for verification
      if (usersWithPaidMonths.length > 0 && feeCollectorTrimmed) {
        console.log(`🔍 Sample user IDs found:`, usersWithPaidMonths.slice(0, 5));
      }
    }
    
    // Base query - include users with paid OR partial status
    // CRITICAL: 'partial' users show in BOTH Paid tab (because they made payment) 
    // AND Balance tab (because they have remaining amount)
    let query = {
      $and: [
        {
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
        },
        {
          // Include both 'paid' and 'partial' users
          // Paid tab = Users who have made payments (including partial)
          status: { $in: ['paid', 'partial'] }
        }
      ]
    };
    
    // CRITICAL: If feeCollector filter is provided:
    // - Income calculation: Only count payments where receivedBy matches feeCollector (already done above)
    // - Paid users list: Show users where receivedBy matches feeCollector (payment receiver)
    // This ensures payments show up for the employee who actually received them
    const feeCollectorTrimmed = feeCollector ? feeCollector.trim() : null;
    if (feeCollectorTrimmed) {
      // Use receivedBy filtering (already done in usersWithPaidMonths above)
      // This ensures employee sees users whose payments they actually received
      console.log(`🔒 Filtering /api/users/paid by receivedBy: ${feeCollectorTrimmed} (for paid users list)`);
      console.log(`💰 Income calculation uses receivedBy (already filtered above)`);
      console.log(`📋 Users with payments received by ${feeCollectorTrimmed}: ${usersWithPaidMonths.length}`);
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
        query.$and.push({ assignTo: { $regex: new RegExp(`^${assignToTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/users/paid by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    // Add user ID filter if we have users with paid months
    if ((paymentDate || (fromDate && toDate)) && userIds.length > 0) {
      const objectIds = userIds.map(id => new ObjectId(id));
      query.$and.push({ _id: { $in: objectIds } });
      console.log(`🔍 Filtering users with IDs:`, objectIds.map(id => id.toString()));
    } else if ((paymentDate || (fromDate && toDate)) && userIds.length === 0) {
      // No users found for this payment date/range
      return res.status(200).json({
        success: true,
        data: [],
        totalCount: 0,
        page,
        limit
      });
    } else if (!paymentDate && !fromDate && !toDate && usersWithPaidMonths.length > 0) {
      // No date filter - filter by users with paid months (already filtered by receivedBy if feeCollector provided)
      const objectIds = usersWithPaidMonths.map(id => {
        try {
          return new ObjectId(id);
        } catch (e) {
          return id;
        }
      });
      query.$and.push({ _id: { $in: objectIds } });
      console.log(`🔍 Filtering by users with paid months: ${objectIds.length} users`);
    } else if (!paymentDate && !fromDate && !toDate && usersWithPaidMonths.length === 0) {
      // No users with paid months from vouchers - return empty
      console.log('📋 No users found with paid months matching the filter criteria');
      return res.status(200).json({
        success: true,
        data: [],
        totalCount: 0,
        totalCollectionAmount, // Return 0 collection if no users
        page,
        limit
      });
    }
    
    const totalCount = await usersCollection.countDocuments(query);
    const users = await usersCollection
      .find(query)
      .sort({ userName: 1 })
      .skip((page - 1) * limit)
      .limit(limit)
      .toArray();
    
    console.log(`Paid users: ${users.length} found, ${totalCount} total`);
    
    res.status(200).json({
      success: true,
      data: users,
      totalCount,
      totalCollectionAmount, // Total collection for date range filter
      page,
      limit
    });
  } catch (error) {
    console.error('Error fetching paid users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching paid users',
      error: error.message
    });
  }
});

// GET my collection - Returns specific payments made by a collector with accurate dates
app.get('/api/collections/my-collection', async (req, res) => {
  try {
    const vouchersCollection = db.collection('vouchers');
    const usersCollection = db.collection('users');
    const { fromDate, toDate, collector } = req.query;
    
    if (!collector) {
      return res.status(400).json({
        success: false,
        message: 'Collector name is required'
      });
    }
    
    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: 'Date range (fromDate and toDate) is required'
      });
    }
    
    console.log(`📊 Fetching My Collection for "${collector}" from ${fromDate} to ${toDate}`);
    
    const collectorTrimmed = collector.trim();
    const collectorRegex = new RegExp(`^${collectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i');
    
    // For backward compatibility: Also match "Myself" if searching for admin
    const isAdminSearch = /^admin$/i.test(collectorTrimmed);
    const shouldMatchMyself = isAdminSearch;
    
    if (shouldMatchMyself) {
      console.log(`🔧 Backward compatibility enabled: Will also match "Myself" payments for admin`);
    }
    
    // Get all vouchers with paid/partial months
    const vouchers = await vouchersCollection.find({
      'months.status': { $in: ['paid', 'partial'] }
    }).toArray();
    
    console.log(`🔍 Total vouchers with paid/partial months: ${vouchers.length}`);
    
    const collections = [];
    let totalAmount = 0;
    let skippedCount = 0;
    let matchedMonthsCount = 0;
    
    // Get all users for mapping
    const allUsers = await usersCollection.find({}).toArray();
    const userMap = new Map();
    allUsers.forEach(user => {
      userMap.set(user._id.toString(), user);
    });
    
    for (const voucher of vouchers) {
      const months = Array.isArray(voucher.months) ? voucher.months : [];
      const user = userMap.get(voucher.userId?.toString());
      const userName = user?.userName || 'Unknown User';
      
      for (const month of months) {
        if (!['paid', 'partial'].includes(month.status)) continue;
        
        matchedMonthsCount++;
        
        // Normalize date helper
        const normalizeDate = (dateValue) => {
          if (!dateValue) return null;
          try {
            const d = new Date(dateValue);
            // Convert to Pakistan time YYYY-MM-DD
            const pktDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Karachi' }).format(d);
            return pktDate;
          } catch (e) {
            if (typeof dateValue === 'string' && dateValue.includes('T')) {
              return dateValue.split('T')[0];
            }
            return null;
          }
        };
        
        // Check paymentHistory FIRST (more specific and accurate)
        const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];
        let addedFromHistory = false;
        
        for (const payment of paymentHistory) {
          const histReceivedBy = payment.receivedBy || '';
          const histMatchesCollector = collectorRegex.test(histReceivedBy) || 
                                       (shouldMatchMyself && histReceivedBy.toLowerCase() === 'myself');
          
          if (histMatchesCollector && payment.date && payment.amount) {
            const dateStr = normalizeDate(payment.date);
            
            // Check if date is in range
            if (dateStr && dateStr >= fromDate && dateStr <= toDate) {
              collections.push({
                userName,
                amount: payment.amount || 0,
                date: dateStr,
                month: month.month,
                receivedBy: histReceivedBy
              });
              totalAmount += payment.amount || 0;
              addedFromHistory = true;
              
              if (collections.length <= 3) {
                console.log(`✅ Added from history: ${userName} - ${month.month} - Rs ${payment.amount} - ${dateStr} - receivedBy: ${histReceivedBy}`);
              }
            } else {
              if (skippedCount < 3) {
                console.log(`⏭️ Skipped (date out of range): ${userName} - ${dateStr} not in ${fromDate} to ${toDate}`);
                skippedCount++;
              }
            }
          } else if (histMatchesCollector) {
            if (skippedCount < 3) {
              console.log(`⏭️ Skipped (missing data): ${userName} - date: ${payment.date ? 'yes' : 'no'}, amount: ${payment.amount ? 'yes' : 'no'}`);
              skippedCount++;
            }
          }
        }
        
        // Only check month.receivedBy if NOT already added from paymentHistory
        // This avoids duplicate entries for the same payment
        if (!addedFromHistory) {
          const monthReceivedBy = month.receivedBy || '';
          const monthMatchesCollector = collectorRegex.test(monthReceivedBy) || 
                                        (shouldMatchMyself && monthReceivedBy.toLowerCase() === 'myself');
          
          if (monthMatchesCollector) {
            // Get payment date from month
            const paymentDate = month.createdAt || month.date;
            const dateStr = normalizeDate(paymentDate);
            
            // Check if date is in range
            if (dateStr && dateStr >= fromDate && dateStr <= toDate) {
              collections.push({
                userName,
                amount: month.paidAmount || 0,
                date: dateStr,
                month: month.month,
                receivedBy: monthReceivedBy
              });
              totalAmount += month.paidAmount || 0;
              
              if (collections.length <= 3) {
                console.log(`✅ Added from month: ${userName} - ${month.month} - Rs ${month.paidAmount} - ${dateStr} - receivedBy: ${monthReceivedBy}`);
              }
            } else {
              if (skippedCount < 3) {
                console.log(`⏭️ Skipped (date out of range): ${userName} - ${dateStr} not in ${fromDate} to ${toDate}`);
                skippedCount++;
              }
            }
          } else if (monthReceivedBy && !monthMatchesCollector) {
            if (skippedCount < 3) {
              console.log(`⏭️ Skipped (receivedBy mismatch): ${userName} - receivedBy: "${monthReceivedBy}" != "${collectorTrimmed}"`);
              skippedCount++;
            }
          }
        }
      }
    }
    
    // Sort by date (most recent first)
    collections.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
    
    console.log(`\n📊 My Collection Summary:`);
    console.log(`   Collector: "${collectorTrimmed}"`);
    console.log(`   Date Range: ${fromDate} to ${toDate}`);
    console.log(`   Total vouchers checked: ${vouchers.length}`);
    console.log(`   Total paid/partial months: ${matchedMonthsCount}`);
    console.log(`   ✅ Payments found: ${collections.length}`);
    console.log(`   💰 Total Amount: Rs ${totalAmount}`);
    console.log(`   ⏭️ Items skipped: ${skippedCount}+\n`);
    
    res.status(200).json({
      success: true,
      data: collections,
      totalAmount,
      count: collections.length
    });
  } catch (error) {
    console.error('Error fetching my collection:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching collection',
      error: error.message
    });
  }
});

// GET unpaid users (with date filter and pagination)
app.get('/api/users/unpaid', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const expiryDate = req.query.expiryDate; // YYYY-MM-DD format
    const unpaidDate = req.query.unpaidDate; // YYYY-MM-DD format - date user became unpaid
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    const search = req.query.search; // Search by name, phone, userId
    
    // CRITICAL: Check vouchers for unpaid months instead of relying on user-level status
    // This ensures users with any unpaid months are included
    // IMPORTANT: Only include users whose expiry date has passed (after 12 PM on expiry date)
    // This prevents Pay Later users from showing before their expiry
    
    // Calculate cutoff time: today at 12 PM in Pakistan timezone
    const now = new Date();
    const todayNoon = new Date(now);
    todayNoon.setHours(12, 0, 0, 0);
    
    console.log(`⏰ Current time: ${now.toISOString()}`);
    console.log(`⏰ Cutoff time (12 PM today): ${todayNoon.toISOString()}`);
    
    // First, find all vouchers that have at least one unpaid month (not refunded)
    // AND whose expiry date has passed (before today at 12 PM)
    const allVouchersWithUnpaidMonths = await vouchersCollection.find({
      'months': { $elemMatch: { status: 'unpaid', refundDate: { $exists: false }, refundedAmount: { $exists: false } } }
    }).toArray();
    
    // Filter vouchers: only include if expiry date has passed
    const vouchersWithUnpaidMonths = allVouchersWithUnpaidMonths.filter(voucher => {
      if (!voucher.expiryDate) return true; // No expiry date, include it
      
      try {
        // Parse expiry date (can be in multiple formats)
        let expiryDate;
        const expiry = voucher.expiryDate;
        
        if (expiry instanceof Date) {
          expiryDate = new Date(expiry);
        } else if (typeof expiry === 'string') {
          // Try different formats: YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY
          if (/^\d{4}-\d{2}-\d{2}/.test(expiry)) {
            expiryDate = new Date(expiry);
          } else {
            const parts = expiry.split(/[-\/]/);
            if (parts.length >= 3) {
              const [a, b, c] = parts;
              if (a.length === 4) {
                // YYYY-MM-DD
                expiryDate = new Date(a, parseInt(b) - 1, parseInt(c));
              } else {
                // DD-MM-YYYY or DD/MM/YYYY
                expiryDate = new Date(c, parseInt(b) - 1, parseInt(a));
              }
            }
          }
        }
        
        if (!expiryDate || isNaN(expiryDate.getTime())) {
          console.log(`⚠️ Invalid expiry date for voucher ${voucher._id}: ${expiry}`);
          return true; // Can't parse, include it
        }
        
        // Set expiry date to 12 PM (noon) on that day
        expiryDate.setHours(12, 0, 0, 0);
        
        // Only include if expiry date + 12 PM has passed
        const shouldInclude = expiryDate <= now;
        
        if (!shouldInclude) {
          console.log(`🕐 Skipping voucher - not expired yet: ${expiry} (expires at 12 PM, now: ${now.toISOString()})`);
        }
        
        return shouldInclude;
      } catch (error) {
        console.error(`Error parsing expiry date for voucher ${voucher._id}:`, error);
        return true; // Error parsing, include it
      }
    });
    
    const userIdsWithUnpaidMonths = [...new Set(vouchersWithUnpaidMonths.map(v => v.userId))];
    const filteredOut = allVouchersWithUnpaidMonths.length - vouchersWithUnpaidMonths.length;
    console.log(`📊 Unpaid vouchers: ${allVouchersWithUnpaidMonths.length} total, ${vouchersWithUnpaidMonths.length} after expiry filter`);
    console.log(`   🕐 Filtered out ${filteredOut} vouchers (expiry date not reached - before 12 PM on expiry date)`);
    
    // If no users have unpaid months, return empty result
    if (userIdsWithUnpaidMonths.length === 0) {
      console.log(`ℹ️ No users with unpaid months found`);
      return res.status(200).json({ success: true, data: [], totalCount: 0, page, limit });
    }
    
    // Base query - active users only, with unpaid months in their vouchers
    let query = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        // Include users who have unpaid months in vouchers
        { _id: { $in: userIdsWithUnpaidMonths.map(id => new ObjectId(id)) } }
      ]
    };
    
    console.log(`📊 Unpaid query: Fetching ${userIdsWithUnpaidMonths.length} users with unpaid voucher months`);
    
    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      if (feeCollectorTrimmed) {
      query.$and.push({ feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/users/unpaid by fee collector (case-insensitive): ${feeCollectorTrimmed}`);
      }
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
        query.$and.push({ assignTo: { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/users/unpaid by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    // Search filter - search by userName, simNo, whatsappNo, userId, streetName
    if (search) {
      const searchTrimmed = search.trim();
      if (searchTrimmed) {
        const searchRegex = new RegExp(searchTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        query.$and.push({
          $or: [
            { userName: searchRegex },
            { simNo: searchRegex },
            { whatsappNo: searchRegex },
            { userId: searchRegex },
            { streetName: searchRegex }
          ]
        });
        console.log(`🔍 Searching unpaid users by: "${searchTrimmed}"`);
      }
    }
    
    // No need to filter by user IDs from vouchers - query already filters by status='unpaid'
    // Date filters will be applied below if needed
    
    // If unpaidDate filter is provided, match users who BECAME unpaid on that calendar day
    if (unpaidDate) {
      const [yearStr, monthStr, dayStr] = String(unpaidDate).split('-');
      const year = parseInt(yearStr, 10);
      const month = parseInt(monthStr, 10) - 1; // JS Date month is 0-indexed
      const day = parseInt(dayStr, 10);
      
      if (!Number.isNaN(year) && !Number.isNaN(month) && !Number.isNaN(day)) {
        // Match strictly by Asia/Karachi local calendar day
        const formatLocalYMD = (date) => {
          try {
            return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Karachi' }).format(date);
          } catch (err) {
            return date.toISOString().split('T')[0];
          }
        };
        const normalizeToLocalYMD = (value) => {
          if (!value) return null;
          if (value instanceof Date) return formatLocalYMD(value);
          if (typeof value === 'string') {
            const str = value.trim();
            if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
            const parts = str.split(/[-\/]/);
            if (parts.length === 3) {
              let [a, b, c] = parts;
              if (a.length === 4) return `${a}-${b.padStart(2, '0')}-${c.padStart(2, '0')}`;
              // Convert DD-MM-YYYY or DD/MM/YYYY to YYYY-MM-DD
              return `${c}-${b.padStart(2, '0')}-${a.padStart(2, '0')}`;
            }
          }
          return null;
        };
        const matchesTargetDate = (value) => {
          const local = normalizeToLocalYMD(value);
          return local === unpaidDate;
        };

        // First, try to match by users.unpaidSince (most reliable going forward)
        const unpaidSinceCandidates = await usersCollection.find({
          status: 'unpaid',
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ],
          unpaidSince: { $exists: true }
        }).project({ _id: 1, unpaidSince: 1 }).toArray();

        const idsByUnpaidSince = unpaidSinceCandidates
          .filter(u => matchesTargetDate(u.unpaidSince))
          .map(u => u._id);

        if (idsByUnpaidSince.length > 0) {
          // CRITICAL: Add _id filter to $and array to combine with feeCollector/assignTo filters
          query.$and.push({ _id: { $in: idsByUnpaidSince } });
          console.log(`🔍 Unpaid users by unpaidSince=${unpaidDate}: ${idsByUnpaidSince.length}`);
        } else {
          // Fallback for older records: look at voucher months CREATED that day with unpaid status
          const vouchers = await vouchersCollection.find({ 'months.status': 'unpaid' }).toArray();
          const filtered = vouchers.filter((voucher) => {
            const months = Array.isArray(voucher.months) ? voucher.months : [];
            const unpaidMonths = months.filter(m => m && m.status === 'unpaid' && m.createdAt);
            if (unpaidMonths.length === 0) return false;
            // Find earliest createdAt among unpaid months
            const earliest = unpaidMonths.reduce((min, m) => {
              const d = new Date(m.createdAt);
              return (!min || d < min) ? d : min;
            }, null);
            return earliest ? matchesTargetDate(earliest) : false;
          });
          const userIds = filtered.map(v => v.userId);
          if (userIds.length === 0) {
            return res.status(200).json({ success: true, data: [], totalCount: 0, page, limit });
          }
          const objectIds = userIds.map(id => new ObjectId(id));
          // CRITICAL: Add _id filter to $and array to combine with feeCollector/assignTo filters
          query.$and.push({ _id: { $in: objectIds } });
          console.log(`🔍 Unpaid users by vouchers for unpaidDate=${unpaidDate}: ${userIds.length}`);
        }
      } else {
        console.log(`⚠️ Invalid unpaidDate received: ${unpaidDate}`);
      }
    }
    // Else, if expiry date filter is provided, check both vouchers and users collections (legacy behavior)
    else if (expiryDate) {
      const [year, month, day] = expiryDate.split('-');
      const dateWithHyphen = `${day}-${month}-${year}`;
      const dateWithSlash = `${day}/${month}/${year}`;
      const isoFormat = expiryDate; // YYYY-MM-DD
      console.log(`📅 Date filter: ${expiryDate} → formats: ${dateWithHyphen}, ${dateWithSlash}, ${isoFormat}`);
      
      // Find vouchers with matching expiry date
      const vouchers = await vouchersCollection.find({
        expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] }
      }).toArray();
      
      const userIdsFromVouchers = vouchers.map(v => v.userId);
      console.log(`📋 Found ${userIdsFromVouchers.length} vouchers with expiry date ${expiryDate}`);
      
      // Also check users collection for expiryDate field directly
      // This handles users who have expiryDate stored directly in their document
      if (userIdsFromVouchers.length > 0) {
        query.$and.push({
          $or: [
            { _id: { $in: userIdsFromVouchers.map(id => new ObjectId(id)) } },
            { expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] } }
          ]
        });
      } else {
        // No vouchers found, but check users collection directly
        query.$and.push({
          expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] }
        });
      }
      
      console.log(`🔍 Unpaid Query with date filter:`, JSON.stringify(query, null, 2));
    }
    
    // Log final query for debugging
    console.log(`🔍 Final Unpaid Query:`, JSON.stringify(query, null, 2));
    console.log(`🔍 Query params - feeCollector: ${feeCollector || 'none'}, assignTo: ${assignTo || 'none'}`);
    
    const totalCount = await usersCollection.countDocuments(query);
    const users = await usersCollection
      .find(query)
      .sort({ userName: 1 })
      .skip((page - 1) * limit)
      .limit(limit)
      .toArray();
    
    // No need for final filter - we already filtered by vouchers with unpaid months
    const finalFilteredUsers = users;
    
    console.log(`Unpaid users: ${finalFilteredUsers.length} found, ${totalCount} total`);
    if (feeCollector || assignTo) {
      console.log(`🔒 Filtered users - feeCollector: ${feeCollector || 'none'}, assignTo: ${assignTo || 'none'}`);
      finalFilteredUsers.forEach(u => {
        console.log(`  - ${u.userName}: feeCollector=${u.feeCollector || 'none'}, assignTo=${u.assignTo || 'none'}`);
      });
    }
    
    res.status(200).json({
      success: true,
      data: finalFilteredUsers,
      totalCount: finalFilteredUsers.length < users.length ? totalCount - (users.length - finalFilteredUsers.length) : totalCount,
      page,
      limit
    });
  } catch (error) {
    console.error('Error fetching unpaid users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching unpaid users',
      error: error.message
    });
  }
});

// GET reversed users (refunded months) - FROM REFUNDS COLLECTION
app.get('/api/users/reversed', async (req, res) => {
  try {
    const refundsCollection = db.collection('refunds');
    const usersCollection = db.collection('users');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const expiryDate = req.query.expiryDate; // YYYY-MM-DD format
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    
    console.log('🔄 Fetching reversed (refunded) users from REFUNDS collection...');
    
    // Find all refunds
    let refundQuery = {};
    
    // Apply expiry date filter if provided (optional - refunds don't have expiry date)
    // This filter is kept for API compatibility but may not be used
    if (expiryDate) {
      console.log(`Date filter: ${expiryDate} (not applied to refunds collection)`);
    }
    
    const allRefunds = await refundsCollection.find(refundQuery).toArray();
    console.log(`📋 Found ${allRefunds.length} refund records`);
    
    if (allRefunds.length === 0) {
      return res.status(200).json({
        success: true,
        data: [],
        totalCount: 0,
        page,
        limit
      });
    }
    
    // Get unique user IDs from refunds
    const userIds = [...new Set(allRefunds.map(r => r.userId))];
    console.log(`👥 Unique users with refunds: ${userIds.length}`);
    
    // Build user query with filters
    let userQuery = {
      _id: { $in: userIds.map(id => new ObjectId(id)) },
      $and: [
        {
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
        }
      ]
    };
    
    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      if (feeCollectorTrimmed) {
        userQuery.$and.push({ feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/users/reversed by fee collector (case-insensitive): ${feeCollectorTrimmed}`);
      }
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
        userQuery.$and.push({ assignTo: { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/users/reversed by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    // First, get all matching users to calculate totalCount
    const allMatchingUsers = await usersCollection.find(userQuery).toArray();
    const totalCount = allMatchingUsers.length;
    
    // Then apply pagination
    const paginatedUserIds = allMatchingUsers
      .slice((page - 1) * limit, page * limit)
      .map(u => u._id);
    
    const users = await usersCollection.find({
      _id: { $in: paginatedUserIds }
    }).toArray();
    
    // Add filterType and calculate reversed amount for each user
    const usersWithReversedData = users.map(user => {
      const userRefunds = allRefunds.filter(r => r.userId === user._id.toString());
      let reversedAmount = 0;
      
      // Calculate total refunded amount from all refund records
      userRefunds.forEach(refund => {
        if (refund.refundedMonths && Array.isArray(refund.refundedMonths)) {
          refund.refundedMonths.forEach(month => {
            // Use refundedAmount which is (paidAmount + remainingAmount)
            reversedAmount += Number(month.refundedAmount || 0);
          });
        }
      });
      
      console.log(`💰 User ${user.userName}: Total refunded = Rs ${reversedAmount}`);
      
      return {
        ...user,
        filterType: 'reversed',
        amount: reversedAmount,
        remainingAmount: reversedAmount
      };
    });
    
    console.log(`✅ Returning ${usersWithReversedData.length} reversed users for page ${page}`);
    
    res.status(200).json({
      success: true,
      data: usersWithReversedData,
      totalCount,
      page,
      limit
    });
  } catch (error) {
    console.error('❌ Error fetching reversed users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching reversed users',
      error: error.message
    });
  }
});

// GET balance users (partial payment users with date filter and pagination)
app.get('/api/balances', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const expiryDate = req.query.expiryDate; // YYYY-MM-DD format
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    const search = req.query.search; // Search by name, phone, userId
    
    // CRITICAL: Balance tab should show ONLY users with status 'partial'
    // Simple logic: status = 'partial' means user has made some payment but has remaining amount
    // No need to check vouchers - user status is the source of truth
    
    // Base query - include users with status 'partial' OR 'superbalance'
    // 'partial' = normal partial payment
    // 'superbalance' = advance payment (all months balance button clicked)
    let query = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        // CRITICAL: Show users with 'partial' OR 'superbalance' status
        // 'superbalance' = advance payment tracking (shows ONLY in Balance tab)
        { 
          $or: [
            { status: 'partial', remainingAmount: { $gt: 0 } },
            { status: 'superbalance' }
          ]
        }
      ]
    };
    
    console.log(`📊 Balance query: Fetching users with status='partial' OR 'superbalance'`);
    
    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      if (feeCollectorTrimmed) {
      query.$and.push({ feeCollector: { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/balances by fee collector (case-insensitive): ${feeCollectorTrimmed}`);
      }
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
        query.$and.push({ assignTo: { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') } });
        console.log(`🔒 STRICT: Filtering /api/balances by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    // Search filter - search by userName, simNo, whatsappNo, userId, streetName
    if (search) {
      const searchTrimmed = search.trim();
      if (searchTrimmed) {
        const searchRegex = new RegExp(searchTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        query.$and.push({
          $or: [
            { userName: searchRegex },
            { simNo: searchRegex },
            { whatsappNo: searchRegex },
            { userId: searchRegex },
            { streetName: searchRegex }
          ]
        });
        console.log(`🔍 Searching balance users by: "${searchTrimmed}"`);
      }
    }
    
    // If expiry date filter is provided, check both vouchers and users collections
    if (expiryDate) {
      const [year, month, day] = expiryDate.split('-');
      const dateWithHyphen = `${day}-${month}-${year}`;
      const dateWithSlash = `${day}/${month}/${year}`;
      const isoFormat = expiryDate; // YYYY-MM-DD
      console.log(`📅 Balance users date filter: ${expiryDate} → formats: ${dateWithHyphen}, ${dateWithSlash}, ${isoFormat}`);
      
      // Find vouchers with matching expiry date
      const vouchers = await vouchersCollection.find({
        expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] }
      }).toArray();
      
      const userIdsFromVouchers = vouchers.map(v => v.userId);
      console.log(`📋 Found ${userIdsFromVouchers.length} vouchers with expiry date ${expiryDate}`);
      
      // Also check users collection for expiryDate field directly
      if (userIdsFromVouchers.length > 0) {
        query.$and.push({
          $or: [
            { _id: { $in: userIdsFromVouchers.map(id => new ObjectId(id)) } },
            { expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] } }
          ]
        });
      } else {
        // No vouchers found, but check users collection directly
        query.$and.push({
          expiryDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] }
        });
      }
      
      console.log(`🔍 Balance Query with date filter:`, JSON.stringify(query, null, 2));
    }
    
    const totalCount = await usersCollection.countDocuments(query);
    const users = await usersCollection
      .find(query)
      .sort({ userName: 1 })
      .skip((page - 1) * limit)
      .limit(limit)
      .toArray();
    
    console.log(`Balance users: ${users.length} found, ${totalCount} total`);
    
    res.status(200).json({
      success: true,
      data: users,
      totalCount,
      page,
      limit
    });
  } catch (error) {
    console.error('Error fetching balance users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching balance users',
      error: error.message
    });
  }
});

// GET expiring soon users
// Supports optional ?date=YYYY-MM-DD (calendar day in PKT). If absent, defaults to TOMORROW.
app.get('/api/users/expiring-soon', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    // Use PKT (UTC+05:00) day math so the calendar day uses your timezone
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();
    
    // If specific date is requested, filter by that date
    const dateParam = req.query.date; // YYYY-MM-DD
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    let targetY, targetM, targetD;
    let filterByDate = false;
    
    if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(String(dateParam))) {
      filterByDate = true;
      const [yyyy, mm, dd] = String(dateParam).split('-');
      targetY = parseInt(yyyy, 10);
      targetM = parseInt(mm, 10) - 1; // JS months 0-based
      targetD = parseInt(dd, 10);
      
      // Guard: if target day is BEFORE today's PKT day, return no results (don't show past dates)
      const isBeforeToday =
        (targetY < todayY) ||
        (targetY === todayY && (targetM < todayM || (targetM === todayM && targetD < todayD)));
      
      if (isBeforeToday) {
        return res.status(200).json({
          success: true,
          count: 0,
          data: []
        });
      }
    }
    
    // Fetch users based on whether specific date is requested
    // If date is provided: fetch ALL active users (we'll filter by date later)
    // If no date: only fetch users marked by cron job with showInExpiringSoon flag
    // CRITICAL: Include ALL statuses for Expiring Soon (paid, unpaid, partial, pending, superbalance)
    // This is a REMINDER list - users should see who's expiring tomorrow regardless of payment status
    const query = {
      status: { $in: ['paid', 'partial', 'unpaid', 'pending', 'superbalance'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    
    // Only filter by showInExpiringSoon flag when no specific date is requested
    if (!filterByDate) {
      query.showInExpiringSoon = true;
    }
    
    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    if (feeCollector) {
      const feeCollectorTrimmed = feeCollector.trim();
      if (feeCollectorTrimmed) {
      query.feeCollector = { $regex: new RegExp(`^${feeCollectorTrimmed}$`, 'i') };
        console.log(`🔒 STRICT: Filtering /api/users/expiring-soon by fee collector (case-insensitive): ${feeCollectorTrimmed}`);
      }
    }
    
    // STRICT: Filter by assignTo (technician) if provided (case-insensitive) - ALWAYS apply
    const assignTo = req.query.assignTo; // Technician assignment filter
    if (assignTo) {
      const assignToTrimmed = assignTo.trim();
      if (assignToTrimmed) {
        query.assignTo = { $regex: new RegExp(`^${assignToTrimmed}$`, 'i') };
        console.log(`🔒 STRICT: Filtering /api/users/expiring-soon by assignTo (technician, case-insensitive): ${assignToTrimmed}`);
      }
    }
    
    const usersAll = await usersCollection.find(query).toArray();

    // Helper: parse expiryDate to PKT Y/M/D (supports DD-MM-YYYY and DD/MM/YYYY, and ISO fallback)
    const toPKT_YMD = (dateObj) => {
      const pkt = new Date(dateObj.getTime() + PKT_OFFSET_MIN * 60000);
      return { y: pkt.getUTCFullYear(), m: pkt.getUTCMonth(), d: pkt.getUTCDate() };
    };
    const parseExpiryYMD = (exp) => {
      if (!exp) return null;
      if (exp instanceof Date) return toPKT_YMD(exp);
      if (typeof exp === 'string') {
        const m1 = exp.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/); // DD-MM-YYYY or DD/MM/YYYY
        if (m1) {
          const d = parseInt(m1[1], 10);
          const m = parseInt(m1[2], 10) - 1;
          const y = parseInt(m1[3], 10);
          const dt = new Date(Date.UTC(y, m, d));
          return toPKT_YMD(dt);
        }
        const m2 = exp.match(/^(\d{4})-(\d{2})-(\d{2})/); // ISO-like
        if (m2) {
          const y = parseInt(m2[1], 10);
          const m = parseInt(m2[2], 10) - 1;
          const d = parseInt(m2[3], 10);
          const dt = new Date(Date.UTC(y, m, d));
          return toPKT_YMD(dt);
        }
        const d2 = new Date(exp);
        if (!isNaN(d2.getTime())) return toPKT_YMD(d2);
        return null;
      }
      return null;
    };

    const mapped = usersAll.map(u => ({ u, ymd: parseExpiryYMD(u.expiryDate) }));
    
    // Filter by date only if date parameter was provided
    let filtered;
    if (filterByDate) {
      // Specific date requested - filter by that date
      filtered = mapped.filter(({ ymd }) => ymd && ymd.y === targetY && ymd.m === targetM && ymd.d === targetD);
    } else {
      // No date parameter - show users expiring within next 2 days (today and tomorrow)
      // This prevents showing users with incorrectly set flags
      const tomorrowInPKT = new Date(Date.UTC(todayY, todayM, todayD) + 24 * 60 * 60 * 1000);
      const tomorrowY = tomorrowInPKT.getUTCFullYear();
      const tomorrowM = tomorrowInPKT.getUTCMonth();
      const tomorrowD = tomorrowInPKT.getUTCDate();
      
      filtered = mapped.filter(({ ymd }) => {
        if (!ymd) return false;
        // Show users expiring TOMORROW
        // This includes Pay Later users created today
        const isTomorrow = (ymd.y === tomorrowY && ymd.m === tomorrowM && ymd.d === tomorrowD);
        return isTomorrow;
      });
    }
    
    const sorted = filtered.sort((a, b) => {
      // Sort by userName A-Z
      const nameA = (a.u.userName || '').toLowerCase();
      const nameB = (b.u.userName || '').toLowerCase();
      return nameA.localeCompare(nameB);
    });

    // Calculate days left using PKT midnights
    const MS_PER_DAY = 24 * 60 * 60 * 1000;
    const todayPKTMidUTC = Date.UTC(todayY, todayM, todayD) - PKT_OFFSET_MIN * 60000;
    const usersWithDaysLeft = sorted.map(({ u, ymd }) => {
      const expPKTMidUTC = Date.UTC(ymd.y, ymd.m, ymd.d) - PKT_OFFSET_MIN * 60000;
      const daysLeft = Math.round((expPKTMidUTC - todayPKTMidUTC) / MS_PER_DAY);
      const expAsDate = new Date(Date.UTC(ymd.y, ymd.m, ymd.d));
      return { ...u, expiryDate: expAsDate.toISOString(), daysLeft };
    });

    res.status(200).json({
      success: true,
      count: usersWithDaysLeft.length,
      data: usersWithDaysLeft
    });
  } catch (error) {
    console.error('Error fetching expiring users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expiring users',
      error: error.message
    });
  }
});

// GET deactivated users
app.get('/api/users/deactivated', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const users = await usersCollection.find({
      status: 'inactive'
    }).sort({ deactivatedDate: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      count: users.length,
      data: users
    });
  } catch (error) {
    console.error('Error fetching deactivated users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching deactivated users',
      error: error.message
    });
  }
});

// GET user transactions by user ID
app.get('/api/transactions/:userId', ensureDbConnection, async (req, res) => {
  try {
    const { userId } = req.params;
    
    // Find all vouchers for this user (paid, partial, or unpaid)
    const vouchers = await vouchersCollection.find({
      userId: userId
    }).sort({ date: -1 }).toArray();
    
    // Transform vouchers to transaction format
    const transactions = vouchers.map(voucher => ({
      _id: voucher._id,
      date: voucher.date || voucher.createdAt || voucher.paymentDate,
      amount: voucher.paidAmount || voucher.packageFee || 0,
      month: voucher.month || new Date(voucher.date || voucher.createdAt).toLocaleDateString('en-US', { month: 'long', year: 'numeric' }),
      paymentMethod: voucher.paymentMethod || 'Cash',
      status: voucher.status || 'unpaid'
    }));
    
    res.status(200).json({
      success: true,
      count: transactions.length,
      data: transactions
    });
  } catch (error) {
    console.error('Error fetching user transactions:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching user transactions',
      error: error.message
    });
  }
});

// GET income transactions
app.get('/api/transactions/income', async (req, res) => {
  try {
    const transactionsCollection = db.collection('transactions');
    const transactions = await transactionsCollection.find({
      type: 'income'
    }).sort({ paymentDate: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      count: transactions.length,
      data: transactions
    });
  } catch (error) {
    console.error('Error fetching income:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching income',
      error: error.message
    });
  }
});

// GET all expenses
app.get('/api/expense', ensureDbConnection, async (req, res) => {
  try {
    const expensesCollection = db.collection('expenses');
    const expenses = await expensesCollection.find({}).sort({ date: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      data: expenses
    });
  } catch (error) {
    console.error('Error fetching expenses:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expenses',
      error: error.message
    });
  }
});

// GET expenses by date range
app.get('/api/expense/by-date-range', ensureDbConnection, async (req, res) => {
  try {
    const { fromDate, toDate } = req.query;
    
    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: 'fromDate and toDate are required (format: YYYY-MM-DD)'
      });
    }
    
    const expensesCollection = db.collection('expenses');
    
    // Parse dates and set time to start/end of day for proper range filtering
    const from = new Date(fromDate);
    from.setHours(0, 0, 0, 0);
    
    const to = new Date(toDate);
    to.setHours(23, 59, 59, 999);
    
    console.log(`📅 Fetching expenses from ${fromDate} to ${toDate}`);
    
    const expenses = await expensesCollection.find({
      date: {
        $gte: from,
        $lte: to
      }
    }).sort({ date: -1 }).toArray();
    
    // Calculate total expense
    const totalExpense = expenses.reduce((sum, expense) => sum + (expense.amount || 0), 0);
    
    res.status(200).json({
      success: true,
      count: expenses.length,
      totalExpense: totalExpense,
      data: expenses
    });
  } catch (error) {
    console.error('Error fetching expenses by date range:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expenses by date range',
      error: error.message
    });
  }
});

// GET expenses grouped by month
app.get('/api/expense/by-month', ensureDbConnection, async (req, res) => {
  try {
    const expensesCollection = db.collection('expenses');
    const paidBy = req.query.paidBy; // Fee collector name filter
    
    // Build query with paidBy filter if provided
    const query = {};
    if (paidBy) {
      query.paidBy = { $regex: new RegExp(`^${paidBy.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
      console.log(`💰 Expense by month - filtering by paidBy: ${paidBy}`);
    }
    
    const expenses = await expensesCollection.find(query).sort({ date: -1 }).toArray();
    console.log(`💰 Expense by month - found ${expenses.length} expenses${paidBy ? ` for ${paidBy}` : ''}`);
    
    // Group expenses by month
    const expensesByMonth = {};
    
    expenses.forEach(expense => {
      const date = new Date(expense.date);
      // Format as "Month YYYY" (e.g., "November 2025")
      const monthYear = date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long'
      });
      
      if (!expensesByMonth[monthYear]) {
        expensesByMonth[monthYear] = {
          month: monthYear,
          expenses: [],
          totalAmount: 0,
          count: 0
        };
      }
      
      expensesByMonth[monthYear].expenses.push(expense);
      expensesByMonth[monthYear].totalAmount += expense.amount;
      expensesByMonth[monthYear].count += 1;
    });
    
    // Convert to array and sort by most recent month
    const monthsArray = Object.values(expensesByMonth).sort((a, b) => {
      const dateA = new Date(a.month);
      const dateB = new Date(b.month);
      return dateB.getTime() - dateA.getTime();
    });
    
    // Always include current month if not present
    const currentDate = new Date();
    const currentMonth = currentDate.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'long'
    });
    
    if (!expensesByMonth[currentMonth]) {
      monthsArray.unshift({
        month: currentMonth,
        expenses: [],
        totalAmount: 0,
        count: 0
      });
    }
    
    res.status(200).json({
      success: true,
      data: monthsArray
    });
  } catch (error) {
    console.error('Error fetching expenses by month:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expenses by month',
      error: error.message
    });
  }
});

// GET expenses for a specific month
app.get('/api/expense/:month', ensureDbConnection, async (req, res) => {
  try {
    const { month } = req.params;
    const paidBy = req.query.paidBy; // Fee collector name filter
    
    if (!month) {
      return res.status(400).json({
        success: false,
        message: 'Month parameter is required'
      });
    }
    
    const expensesCollection = db.collection('expenses');
    
    // Build query with paidBy filter if provided
    const query = {};
    if (paidBy) {
      query.paidBy = { $regex: new RegExp(`^${paidBy.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
      console.log(`💰 Expense for month ${month} - filtering by paidBy: ${paidBy}`);
    }
    
    const allExpenses = await expensesCollection.find(query).toArray();
    console.log(`💰 Found ${allExpenses.length} expenses${paidBy ? ` for ${paidBy}` : ''}`);
    
    // Filter expenses for the specified month
    const filteredExpenses = allExpenses.filter(expense => {
      const date = new Date(expense.date);
      const expenseMonth = date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long'
      });
      return expenseMonth === month;
    });
    
    res.status(200).json({
      success: true,
      data: filteredExpenses,
      month
    });
  } catch (error) {
    console.error('Error fetching expenses by month:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expenses by month',
      error: error.message
    });
  }
});

// DEBUG endpoint to check collections
app.get('/api/debug/collections', ensureDbConnection, async (req, res) => {
  try {
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);
    
    // Check expenses collection specifically
    const expensesExists = collectionNames.includes('expenses');
    let expenseCount = 0;
    let sampleExpense = null;
    
    if (expensesExists) {
      expenseCount = await db.collection('expenses').countDocuments();
      sampleExpense = await db.collection('expenses').findOne({});
    }
    
    res.status(200).json({
      success: true,
      database: db.databaseName,
      allCollections: collectionNames,
      expensesCollection: {
        exists: expensesExists,
        initialized: !!expensesCollection,
        count: expenseCount,
        sampleData: sampleExpense
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST add new expense
app.post('/api/transactions/expense', ensureDbConnection, async (req, res) => {
  try {
    const { amount, description, category, paidTo, paidBy, date } = req.body;
    
    // Validate required fields
    if (!amount || !description || !category) {
      return res.status(400).json({
        success: false,
        message: 'Amount, description, and category are required'
      });
    }
    
    // Create expense object
    const expense = {
      amount: parseFloat(amount),
      description,
      category,
      paidTo: paidTo || 'N/A',
      paidBy: paidBy || 'Admin', // Default to Admin if not provided
      date: date ? new Date(date) : new Date(),
      createdAt: new Date()
    };
    
    // Insert into expenses collection
    const result = await expensesCollection.insertOne(expense);
    
    // Get the created expense
    const createdExpense = await expensesCollection.findOne({ _id: result.insertedId });
    
    // Also create a transaction record for consistency
    try {
      const transactionsCollection = db.collection('transactions');
      await transactionsCollection.insertOne({
        amount: expense.amount,
        description: expense.description,
        type: 'expense',
        category: expense.category,
        paidTo: expense.paidTo,
        paidBy: expense.paidBy,
        paymentDate: expense.date,
        createdAt: expense.createdAt
      });
    } catch (transactionError) {
      console.log('⚠️ Could not create transaction record for expense:', transactionError);
      // Continue even if transaction record fails
    }
    
    console.log('✅ Expense added:', createdExpense);
    
    res.status(201).json({
      success: true,
      message: 'Expense added successfully',
      data: createdExpense
    });
  } catch (error) {
    console.error('❌ Error adding expense:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding expense',
      error: error.message
    });
  }
});

// DELETE expense by ID
app.delete('/api/transactions/expense/:id', ensureDbConnection, async (req, res) => {
  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid expense ID'
      });
    }
    
    const result = await expensesCollection.deleteOne({ _id: new ObjectId(id) });
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense not found'
      });
    }
    
    console.log('✅ Expense deleted:', id);
    
    res.status(200).json({
      success: true,
      message: 'Expense deleted successfully'
    });
  } catch (error) {
    console.error('❌ Error deleting expense:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting expense',
      error: error.message
    });
  }
});

// ============ EMPLOYEE EXPENSE API ROUTES ============

// GET employee expenses grouped by month
app.get('/api/employee-expense/by-month', ensureDbConnection, async (req, res) => {
  try {
    const employeeExpenseCollection = db.collection('employee_expense');
    const paidBy = req.query.paidBy; // Fee collector name filter
    
    // Build query with paidBy filter if provided
    const query = {};
    if (paidBy) {
      query.paidBy = { $regex: new RegExp(`^${paidBy.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
      console.log(`💰 Employee expense by month - filtering by paidBy: ${paidBy}`);
    }
    
    const expenses = await employeeExpenseCollection.find(query).sort({ date: -1 }).toArray();
    console.log(`💰 Employee expense by month - found ${expenses.length} expenses${paidBy ? ` for ${paidBy}` : ''}`);
    
    // Group expenses by month
    const expensesByMonth = {};
    
    expenses.forEach(expense => {
      const date = new Date(expense.date);
      // Format as "Month YYYY" (e.g., "November 2025")
      const monthYear = date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long'
      });
      
      if (!expensesByMonth[monthYear]) {
        expensesByMonth[monthYear] = {
          month: monthYear,
          expenses: [],
          totalAmount: 0,
          count: 0
        };
      }
      
      expensesByMonth[monthYear].expenses.push(expense);
      expensesByMonth[monthYear].totalAmount += expense.amount;
      expensesByMonth[monthYear].count += 1;
    });
    
    // Convert to array and sort by most recent month
    const monthsArray = Object.values(expensesByMonth).sort((a, b) => {
      const dateA = new Date(a.month);
      const dateB = new Date(b.month);
      return dateB.getTime() - dateA.getTime();
    });
    
    // Always include current month if not present
    const currentDate = new Date();
    const currentMonth = currentDate.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'long'
    });
    
    if (!expensesByMonth[currentMonth]) {
      monthsArray.unshift({
        month: currentMonth,
        expenses: [],
        totalAmount: 0,
        count: 0
      });
    }
    
    res.status(200).json({
      success: true,
      data: monthsArray
    });
  } catch (error) {
    console.error('Error fetching employee expenses by month:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching employee expenses by month',
      error: error.message
    });
  }
});

// GET employee expenses for a specific month
app.get('/api/employee-expense/:month', ensureDbConnection, async (req, res) => {
  try {
    const { month } = req.params;
    const paidBy = req.query.paidBy; // Fee collector name filter
    
    if (!month) {
      return res.status(400).json({
        success: false,
        message: 'Month parameter is required'
      });
    }
    
    const employeeExpenseCollection = db.collection('employee_expense');
    
    // Build query with paidBy filter if provided
    const query = {};
    if (paidBy) {
      query.paidBy = { $regex: new RegExp(`^${paidBy.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') };
      console.log(`💰 Employee expense for month ${month} - filtering by paidBy: ${paidBy}`);
    }
    
    const allExpenses = await employeeExpenseCollection.find(query).toArray();
    console.log(`💰 Found ${allExpenses.length} employee expenses${paidBy ? ` for ${paidBy}` : ''}`);
    
    // Filter expenses for the specified month
    const filteredExpenses = allExpenses.filter(expense => {
      const date = new Date(expense.date);
      const expenseMonth = date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long'
      });
      return expenseMonth === month;
    });
    
    res.status(200).json({
      success: true,
      data: filteredExpenses,
      month
    });
  } catch (error) {
    console.error('Error fetching employee expenses by month:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching employee expenses by month',
      error: error.message
    });
  }
});

// POST add new employee expense
app.post('/api/employee-expense', ensureDbConnection, async (req, res) => {
  try {
    const { amount, description, userName, paidTo, paidBy, date } = req.body;
    
    // Validate required fields
    if (!amount || !description || !userName) {
      return res.status(400).json({
        success: false,
        message: 'Amount, description, and user name are required'
      });
    }
    
    // Create expense object
    const expense = {
      amount: parseFloat(amount),
      description,
      category: 'General',
      userName: userName.trim(),
      paidTo: paidTo || 'N/A',
      paidBy: paidBy || 'Admin', // Default to Admin if not provided
      date: date ? new Date(date) : new Date(),
      createdAt: new Date()
    };
    
    // Insert into employee_expense collection
    const employeeExpenseCollection = db.collection('employee_expense');
    const result = await employeeExpenseCollection.insertOne(expense);
    
    // Get the created expense
    const createdExpense = await employeeExpenseCollection.findOne({ _id: result.insertedId });
    
    console.log('✅ Employee expense added:', createdExpense);
    
    res.status(201).json({
      success: true,
      message: 'Employee expense added successfully',
      data: createdExpense
    });
  } catch (error) {
    console.error('❌ Error adding employee expense:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding employee expense',
      error: error.message
    });
  }
});

// DELETE employee expense by ID
app.delete('/api/employee-expense/:id', ensureDbConnection, async (req, res) => {
  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid employee expense ID'
      });
    }
    
    const employeeExpenseCollection = db.collection('employee_expense');
    const result = await employeeExpenseCollection.deleteOne({ _id: new ObjectId(id) });
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Employee expense not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Employee expense deleted successfully'
    });
  } catch (error) {
    console.error('❌ Error deleting employee expense:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting employee expense',
      error: error.message
    });
  }
});

// GET outstanding users
app.get('/api/users/outstanding', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const users = await usersCollection.find({
      expiryDate: { $lt: new Date() }
    }).sort({ expiryDate: 1 }).toArray();
    
    // Calculate months overdue and total outstanding
    const now = new Date();
    const usersWithOutstanding = users.map(user => {
      const monthsOverdue = Math.floor((now - new Date(user.expiryDate)) / (1000 * 60 * 60 * 24 * 30));
      return {
        ...user,
        monthsOverdue: monthsOverdue > 0 ? monthsOverdue : 1,
        totalOutstanding: (user.amount || 0) * (monthsOverdue > 0 ? monthsOverdue : 1)
      };
    });
    
    res.status(200).json({
      success: true,
      count: users.length,
      data: usersWithOutstanding
    });
  } catch (error) {
    console.error('Error fetching outstanding:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching outstanding',
      error: error.message
    });
  }
});

// GET expired users (users whose expiry date has passed)
app.get('/api/users/expired', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    // Find users whose expiry date has passed and are not inactive
    const users = await usersCollection.find({
      expiryDate: { $lt: today.toISOString() },
      serviceStatus: { $ne: 'inactive' }
    }).sort({ expiryDate: -1 }).toArray();
    
    // Add additional information for display
    const expiredUsers = users.map(user => {
      const expiryDate = new Date(user.expiryDate);
      const daysPassed = Math.ceil((today - expiryDate) / (1000 * 60 * 60 * 24));
      return {
        ...user,
        daysPassed,
        expiryDateFormatted: expiryDate.toLocaleDateString('en-US', { day: 'numeric', month: 'long', year: 'numeric' })
      };
    });
    
    res.status(200).json({
      success: true,
      count: expiredUsers.length,
      data: expiredUsers
    });
  } catch (error) {
    console.error('Error fetching expired users:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expired users',
      error: error.message
    });
  }
});

// GET route to fetch a single user by ID (must be after specific routes)
app.get('/api/users/:id', async (req, res) => {
  try {
    // Validate ObjectId format
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid user ID format'
      });
    }

    const user = await usersCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }
    res.status(200).json({
      success: true,
      data: user
    });
  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching user',
      error: error.message
    });
  }
});

// POST create voucher (save to vouchers collection)
app.post('/api/vouchers', async (req, res) => {
  try {
    const vouchersCollection = db.collection('vouchers');

    const {
      userId,
      userName,
      rechargeDate,
      expiryDate,
      months,  // NEW: Support for months array
      packageFee,
      discount,
      paidAmount,
      remainingAmount,
      paymentMethod,
      receivedBy,
      paymentType,
      status,
      month,
      date,
      description
    } = req.body;
    
    // NEW: If months array is provided, create voucher with months array structure
    if (months && Array.isArray(months) && months.length > 0) {
      console.log(`📦 Creating voucher with ${months.length} months array for user ${userName}`);
      
      // Debug: Log receivedBy for first month
      if (months[0]) {
        console.log(`🔍 First month receivedBy check:`, {
          month: months[0].month,
          receivedBy: months[0].receivedBy,
          hasPaymentHistory: !!months[0].paymentHistory,
          paymentHistoryLength: months[0].paymentHistory?.length || 0
        });
      }
      
      // CRITICAL: Sort months by date (FIFO - First In First Out) before storing
      // This ensures months are always stored in chronological order (earliest first)
      // Payment distribution should apply to earliest months first (e.g., Oct before Nov)
      const parseDate = (dateStr) => {
        if (!dateStr) return new Date(0);
        if (dateStr instanceof Date) return dateStr;
        if (typeof dateStr === 'string') {
          // Check if ISO format (YYYY-MM-DD or full ISO string)
          if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
            // ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss.sssZ
            const parsed = new Date(dateStr);
            if (!isNaN(parsed.getTime())) return parsed;
          }
          
          // Try DD-MM-YYYY format
          const parts = dateStr.split('-');
          if (parts.length === 3 && parts[2].length === 4) {
            // DD-MM-YYYY format (last part is year with 4 digits)
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1;
            const year = parseInt(parts[2], 10);
            if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
              return new Date(year, month, day);
            }
          }
          
          // Fallback to standard Date parsing
          const parsed = new Date(dateStr);
          return isNaN(parsed.getTime()) ? new Date(0) : parsed;
        }
        return new Date(0);
      };
      
      const sortedMonths = [...months].sort((a, b) => {
        const dateA = parseDate(a.date || a.createdAt);
        const dateB = parseDate(b.date || b.createdAt);
        return dateA.getTime() - dateB.getTime(); // Ascending: earliest first
      });
      
      console.log(`📅 Months sorted by date (FIFO order):`, sortedMonths.map(m => `${m.month} (${m.date || m.createdAt})`).join(', '));
      
      // Check if user already has a voucher
      const existingVoucher = await vouchersCollection.findOne({ userId });
      
      if (existingVoucher) {
        // Check if any months being paid have reversed status
        const refundsCollection = db.collection('refunds');
        const reversedMonthsPaid = sortedMonths.filter(m => {
          // Find if this month exists in voucher with reversed status
          const existingMonth = existingVoucher.months?.find(em => em.month === m.month);
          return existingMonth && existingMonth.status === 'reversed' && m.status === 'paid';
        });
        
        // If reversed months are being paid, delete from refunds collection
        if (reversedMonthsPaid.length > 0) {
          console.log(`🔄 Marking ${reversedMonthsPaid.length} reversed months as paid`);
          
          // Delete refund records for these months
          for (const month of reversedMonthsPaid) {
            await refundsCollection.updateMany(
              { userId, 'refundedMonths.month': month.month },
              { $pull: { refundedMonths: { month: month.month } } }
            );
          }
          
          // Remove empty refund records
          await refundsCollection.deleteMany({ 
            userId, 
            refundedMonths: { $size: 0 } 
          });
          
          console.log(`✅ Removed reversed months from refunds collection`);
        }
        
        // CRITICAL: Convert all 'superbalance' months to 'unpaid' when new voucher is created
        // This happens when expiry date arrives and new month voucher is generated
        let hasSuperBalanceConverted = false;
        const updatedMonths = sortedMonths.map(month => {
          if (month.status === 'superbalance') {
            console.log(`⚡ Converting superbalance month '${month.month}' to unpaid (new voucher created)`);
            hasSuperBalanceConverted = true;
            return {
              ...month,
              status: 'unpaid',
              paidAmount: 0,
              remainingAmount: month.packageFee - (month.discount || 0),
              paymentHistory: []
            };
          }
          return month;
        });
        
        // Update existing voucher with new months array (in FIFO order)
        const result = await vouchersCollection.updateOne(
          { userId },
          { 
            $set: { 
              months: updatedMonths,
              rechargeDate: rechargeDate || existingVoucher.rechargeDate,
              expiryDate: expiryDate || existingVoucher.expiryDate
            } 
          }
        );
        
        // CRITICAL: If superbalance months were converted, update user status to 'unpaid'
        if (hasSuperBalanceConverted) {
          const usersCollection = db.collection('users');
          const totalRemaining = updatedMonths.reduce((sum, m) => sum + (m.remainingAmount || 0), 0);
          await usersCollection.updateOne(
            { _id: new ObjectId(userId) },
            { 
              $set: { 
                status: 'unpaid',
                remainingAmount: totalRemaining
              } 
            }
          );
          console.log(`✅ User status updated from 'superbalance' to 'unpaid' (remaining: Rs ${totalRemaining})`);
        }
        
        // 💰 UPDATE INCOME: Process payments and update receiver's income (ONLY cashIncome)
        for (const month of sortedMonths) {
          if (month.status === 'paid' || month.status === 'partial') {
            // Check if month has paymentHistory (new structure) or receivedBy (old structure)
            const paymentHistory = month.paymentHistory || [];
            
            if (paymentHistory.length > 0) {
              // New structure: Process each payment in history
              for (const payment of paymentHistory) {
                const receiver = payment.receivedBy || 'Admin';
                const amount = parseFloat(payment.amount) || 0;
                
                if (amount > 0) {
                  await incomesCollection.updateOne(
                    { name: receiver },
                    { 
                      $inc: { cashIncome: amount },
                      $set: { lastUpdated: new Date() },
                      $setOnInsert: { name: receiver, createdAt: new Date() }
                    },
                    { upsert: true }
                  );
                  console.log(`💰 Income updated: ${receiver} +Rs${amount}`);
                }
              }
            } else if (month.receivedBy) {
              // Old structure: Single receivedBy field
              const receiver = month.receivedBy;
              const amount = parseFloat(month.paidAmount) || 0;
              
              if (amount > 0) {
                await incomesCollection.updateOne(
                  { name: receiver },
                  { 
                    $inc: { cashIncome: amount },
                    $set: { lastUpdated: new Date() },
                    $setOnInsert: { name: receiver, createdAt: new Date() }
                  },
                  { upsert: true }
                );
                console.log(`💰 Income updated: ${receiver} +Rs${amount}`);
              }
            }
          }
        }
        
        return res.status(200).json({
          success: true,
          message: 'Voucher updated with months array',
          data: { _id: existingVoucher._id }
        });
      } else {
        // Create new voucher with months array (in FIFO order)
        const newVoucher = {
          userId,
          userName,
          rechargeDate: rechargeDate || null,
          expiryDate: expiryDate || null,
          months: sortedMonths,
          createdAt: new Date()
        };
        
        const result = await vouchersCollection.insertOne(newVoucher);
        
        // 💰 UPDATE INCOME: Process payments and update receiver's income (ONLY cashIncome)
        for (const month of sortedMonths) {
          if (month.status === 'paid' || month.status === 'partial') {
            // Check if month has paymentHistory (new structure) or receivedBy (old structure)
            const paymentHistory = month.paymentHistory || [];
            
            if (paymentHistory.length > 0) {
              // New structure: Process each payment in history
              for (const payment of paymentHistory) {
                const receiver = payment.receivedBy || 'Admin';
                const amount = parseFloat(payment.amount) || 0;
                
                if (amount > 0) {
                  await incomesCollection.updateOne(
                    { name: receiver },
                    { 
                      $inc: { cashIncome: amount },
                      $set: { lastUpdated: new Date() },
                      $setOnInsert: { name: receiver, createdAt: new Date() }
                    },
                    { upsert: true }
                  );
                  console.log(`💰 Income updated: ${receiver} +Rs${amount}`);
                }
              }
            } else if (month.receivedBy) {
              // Old structure: Single receivedBy field
              const receiver = month.receivedBy;
              const amount = parseFloat(month.paidAmount) || 0;
              
              if (amount > 0) {
                await incomesCollection.updateOne(
                  { name: receiver },
                  { 
                    $inc: { cashIncome: amount },
                    $set: { lastUpdated: new Date() },
                    $setOnInsert: { name: receiver, createdAt: new Date() }
                  },
                  { upsert: true }
                );
                console.log(`💰 Income updated: ${receiver} +Rs${amount}`);
              }
            }
          }
        }
        
        return res.status(201).json({
          success: true,
          message: 'Voucher created with months array',
          data: { _id: result.insertedId, ...newVoucher }
        });
      }
    }
    
    // EXISTING: Individual month creation logic (for backward compatibility)

    // Required validation
    if (!userId || !userName || packageFee === undefined || !month) {
      return res.status(400).json({
        success: false,
        message: 'Missing required fields: userId, userName, packageFee, month'
      });
    }

    // Voucher data for this month
    const monthData = {
      month,
      packageFee: parseFloat(packageFee),
      discount: parseFloat(discount) || 0,
      paidAmount: parseFloat(paidAmount) || 0,
      remainingAmount: parseFloat(remainingAmount) || 0,
      paymentMethod: paymentMethod || 'Not Paid',
      receivedBy: receivedBy || 'Myself',
      paymentType: paymentType || 'later',
      status: status || 'unpaid',
      description: description || `${month} - ${status === 'paid' ? 'Paid' : 'Pending'}`,
      date: date ? new Date(date) : new Date(),
      createdAt: new Date()
    };

    // Check if user already has a voucher document
    const existingVoucher = await vouchersCollection.findOne({ userId });

    if (existingVoucher) {
      // Check if the same month already exists
      const monthExists = existingVoucher.months?.some(
        (m) => m.month === month
      );

      if (monthExists) {
        return res.status(400).json({
          success: false,
          message: `Voucher for ${month} already exists for this user.`
        });
      }

      // Add new month to existing document and update dates if provided
      const updateFields = { $push: { months: monthData } };
      if (rechargeDate || expiryDate) {
        updateFields.$set = {};
        if (rechargeDate) updateFields.$set.rechargeDate = rechargeDate;
        if (expiryDate) updateFields.$set.expiryDate = expiryDate;
      }
      
      await vouchersCollection.updateOne(
        { userId },
        updateFields
      );

      res.status(200).json({
        success: true,
        message: `New month (${month}) added to existing voucher.`,
        data: monthData
      });
    } else {
      // Create new voucher document for the user with months array
      const newVoucher = {
        userId,
        userName,
        rechargeDate: rechargeDate || null,
        expiryDate: expiryDate || null,
        months: [monthData],
        createdAt: new Date()
      };

      const result = await vouchersCollection.insertOne(newVoucher);

      res.status(201).json({
        success: true,
        message: 'New voucher document created for user.',
        data: { _id: result.insertedId, ...newVoucher }
      });
    }
  } catch (error) {
    console.error('Error creating voucher:', error);
    res.status(500).json({
      success: false,
      message: 'Error creating voucher',
      error: error.message
    });
  }
});

// PUT update voucher by ID (update months array and optional dates)
app.put('/api/vouchers/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid voucher ID format'
      });
    }

    const vouchersCollection = db.collection('vouchers');
    const { months, rechargeDate, expiryDate } = req.body;
    const updateFields = {};

    if (Array.isArray(months)) {
      // CRITICAL: Sort months by date (FIFO - First In First Out) before storing
      // This ensures months are always stored in chronological order (earliest first)
      // Payment distribution should apply to earliest months first (e.g., Oct before Nov)
      const parseDate = (dateStr) => {
        if (!dateStr) return new Date(0);
        if (dateStr instanceof Date) return dateStr;
        if (typeof dateStr === 'string') {
          // Check if ISO format (YYYY-MM-DD or full ISO string)
          if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
            // ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss.sssZ
            const parsed = new Date(dateStr);
            if (!isNaN(parsed.getTime())) return parsed;
          }
          
          // Try DD-MM-YYYY format
          const parts = dateStr.split('-');
          if (parts.length === 3 && parts[2].length === 4) {
            // DD-MM-YYYY format (last part is year with 4 digits)
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1;
            const year = parseInt(parts[2], 10);
            if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
              return new Date(year, month, day);
            }
          }
          
          // Fallback to standard Date parsing
          const parsed = new Date(dateStr);
          return isNaN(parsed.getTime()) ? new Date(0) : parsed;
        }
        return new Date(0);
      };
      
      const sortedMonths = [...months].sort((a, b) => {
        const dateA = parseDate(a.date || a.createdAt);
        const dateB = parseDate(b.date || b.createdAt);
        return dateA.getTime() - dateB.getTime(); // Ascending: earliest first
      });
      
      console.log(`📅 Backend PUT: Months sorted by date (FIFO order):`, sortedMonths.map(m => `${m.month} (${m.date || m.createdAt})`).join(', '));
      updateFields.months = sortedMonths;
    }
    if (rechargeDate !== undefined) updateFields.rechargeDate = rechargeDate || null;
    if (expiryDate !== undefined) updateFields.expiryDate = expiryDate || null;
    updateFields.updatedAt = new Date();

    if (Object.keys(updateFields).length === 1 && updateFields.updatedAt) {
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }

    const result = await vouchersCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updateFields }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Voucher not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Voucher updated successfully'
    });
  } catch (error) {
    console.error('Error updating voucher:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating voucher',
      error: error.message
    });
  }
});

// GET user transaction history
app.get('/api/users/:id/transactions', async (req, res) => {
  try {
    

    const user = await usersCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    // Get all vouchers for this user
    const vouchersCollection = db.collection('vouchers');
    const vouchers = await vouchersCollection.find({
      userId: req.params.id
    }).sort({ date: 1 }).toArray(); // Sort by 'date' field instead of 'createdAt'

    // Build transaction history
    const transactions = [];
    let runningBalance = 0;

    vouchers.forEach(voucher => {
      // Use voucher.date if available, fallback to createdAt
      const voucherDate = voucher.date ? new Date(voucher.date) : new Date(voucher.createdAt);
      const monthYear = voucherDate.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
      
      // Calculate actual pending amount (packageFee - discount - paidAmount)
      const packageFee = Number(voucher.packageFee || 0);
      const discount = Number(voucher.discount || 0);
      const paidAmount = Number(voucher.paidAmount || 0);
      const remainingAmount = Number(voucher.remainingAmount || 0);
      
      // If remainingAmount is set, use it (for Pay Later vouchers)
      // Otherwise calculate: packageFee - discount
      const actualFee = remainingAmount > 0 ? remainingAmount + paidAmount : packageFee - discount;
      
      // Add package fee as debit (after discount)
      runningBalance -= actualFee;
      transactions.push({
        date: monthYear,
        description: voucher.month || `${voucherDate.toLocaleDateString('en-GB', { month: 'long' })} Fee`,
        debit: actualFee,
        credit: null,
        balance: runningBalance
      });

      // Add payment as credit (if paid)
      if (paidAmount > 0) {
        runningBalance += paidAmount;
        transactions.push({
          date: monthYear,
          description: 'Payment Received',
          debit: null,
          credit: paidAmount,
          balance: runningBalance
        });
      }
    });

    res.status(200).json({
      success: true,
      data: {
        user: {
          _id: user._id,
          userName: user.userName,
          status: user.status
        },
        transactions: transactions,
        currentBalance: runningBalance
      }
    });
  } catch (error) {
    console.error('Error fetching transactions:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching transactions',
      error: error.message
    });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    message: 'Something went wrong!',
    error: err.message
  });
});

// ============ SCHEDULED TASKS ============

// Function to check users expiring TOMORROW (next day) - marks them to show in expiring-soon tab
const checkTomorrowExpiringUsers = async () => {
  try {
    console.log('🕐 Running scheduled task: Checking users expiring TOMORROW...');
    
    // Use PKT timezone
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();
    
    // Calculate tomorrow's date in PKT
    const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
    const tomorrowY = tomorrowDate.getUTCFullYear();
    const tomorrowM = tomorrowDate.getUTCMonth();
    const tomorrowD = tomorrowDate.getUTCDate();
    
    console.log(`📅 Today: ${todayY}-${todayM+1}-${todayD}, Tomorrow: ${tomorrowY}-${tomorrowM+1}-${tomorrowD}`);
    
    // Fetch ALL users and parse their expiry dates
    const usersAll = await usersCollection.find({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending', 'superbalance'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    }).toArray();
    
    const toPKT_YMD = (dateObj) => {
      const pkt = new Date(dateObj.getTime() + PKT_OFFSET_MIN * 60000);
      return { y: pkt.getUTCFullYear(), m: pkt.getUTCMonth(), d: pkt.getUTCDate() };
    };
    
    const parseExpiryYMD = (exp) => {
      if (!exp) return null;
      if (exp instanceof Date) return toPKT_YMD(exp);
      if (typeof exp === 'string') {
        const parts = exp.split('-');
        if (parts.length === 3) {
          const d = parseInt(parts[0], 10);
          const m = parseInt(parts[1], 10) - 1;
          const y = parseInt(parts[2], 10);
          if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
            const dt = new Date(Date.UTC(y, m, d));
            return toPKT_YMD(dt);
          }
        }
        const d2 = new Date(exp);
        if (!isNaN(d2.getTime())) return toPKT_YMD(d2);
        return null;
      }
      return null;
    };
    
    // Find users expiring TOMORROW
    const expiringTomorrowUsers = usersAll
      .map(u => ({ u, ymd: parseExpiryYMD(u.expiryDate) }))
      .filter(({ ymd }) => ymd && ymd.y === tomorrowY && ymd.m === tomorrowM && ymd.d === tomorrowD)
      .map(({ u }) => u);
    
    console.log(`✅ Found ${expiringTomorrowUsers.length} users expiring TOMORROW`);
    
    if (expiringTomorrowUsers.length > 0) {
      // Just mark users to show in Expiring Soon (don't change status or create voucher yet)
      for (const user of expiringTomorrowUsers) {
        console.log(`   - Marking ${user.userName} for Expiring Soon (expires tomorrow: ${user.expiryDate})`);
        
        // Only set the Expiring Soon flag (status stays as paid/partial/pending)
        await usersCollection.updateOne(
          { _id: user._id },
          { 
            $set: { 
              showInExpiringSoon: true
            } 
          }
        );
        
        console.log(`   ✅ ${user.userName} will show in Expiring Soon (status unchanged)`);
      }
    }
    
  } catch (error) {
    console.error('❌ Error in checkTomorrowExpiringUsers:', error);
  }
};

// Function to check and update expiring users (7 days)
const checkExpiringUsers = async () => {
  try {
    console.log('🕐 Running scheduled task: Checking expiring users...');
    
    const today = new Date();
    today.setHours(0, 0, 0, 0);    
    const sevenDaysFromNow = new Date(today);
    sevenDaysFromNow.setDate(sevenDaysFromNow.getDate() + 7);
    sevenDaysFromNow.setHours(23, 59, 59, 999);
    
    // Find users who are paid/partial and expiring within 7 days
    const expiringUsers = await usersCollection.find({
      status: { $in: ['paid', 'partial'] },
      expiryDate: { 
        $gte: today.toISOString(), 
        $lte: sevenDaysFromNow.toISOString() 
      }
    }).toArray();
    
    console.log(`✅ Found ${expiringUsers.length} users expiring within 7 days`);
    
    // Optional: You can add a flag or notification here
    if (expiringUsers.length > 0) {
      expiringUsers.forEach(user => {
        const expiryDate = new Date(user.expiryDate);
        const daysLeft = Math.ceil((expiryDate - today) / (1000 * 60 * 60 * 24));
        console.log(`   - ${user.userName} expires in ${daysLeft} days (${user.expiryDate})`);
      });
    }
    
  } catch (error) {
    console.error('❌ Error in scheduled task:', error);
  }
};

// Function to move users to unpaid on their actual expiry day and create next month voucher
// ACTUAL RECURRING CYCLE FLOW:
// 1. User added: 28/10/2025 (recharge) → 28/11/2025 (expiry) → Status: PAID
// 2. On 27/11/2025 (1 day before expiry): checkTomorrowExpiringUsers() runs at 12 PM
//    - Sets showInExpiringSoon = true (ONLY THIS, no status change)
//    - User appears in "Expiring Soon" tab
//    - Status remains PAID
// 3. On 28/11/2025 (actual expiry day): moveTodayExpiredToUnpaid() runs at 12 PM
//    - Changes status: PAID → UNPAID
//    - Creates December voucher (unpaid month)
//    - Updates expiry: 28/12/2025
//    - Removes showInExpiringSoon flag
//    - User shows in "Unpaid" tab only
// 4. User pays December: Status → PAID, shows in paid-users
// 5. On 27/12/2025 (1 day before): Shows in "Expiring Soon" again
// 6. On 28/12/2025: Moves to unpaid again, creates January voucher
// 7. Cycle repeats monthly
const moveTodayExpiredToUnpaid = async () => {
  try {
    console.log('🕐 Running scheduled task: Moving TODAY/PAST expiring users to unpaid...');
    
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const endOfToday = new Date(today);
    endOfToday.setHours(23, 59, 59, 999);
    
    // Find ALL users (paid/partial/unpaid/pending) and match expiry by PKT calendar-day equality
    // NOTE: Do not use ISO range because expiryDate is stored as string (DD-MM-YYYY or DD/MM/YYYY)
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();

    const usersAll = await usersCollection.find({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending', 'superbalance'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    }).toArray();

    const toPKT_YMD = (dateObj) => {
      const pkt = new Date(dateObj.getTime() + PKT_OFFSET_MIN * 60000);
      return { y: pkt.getUTCFullYear(), m: pkt.getUTCMonth(), d: pkt.getUTCDate() };
    };
    const parseExpiryYMD = (exp) => {
      if (!exp) return null;
      if (exp instanceof Date) return toPKT_YMD(exp);
      if (typeof exp === 'string') {
        const parts = exp.split('-');
        if (parts.length === 3) {
          const [dd, mm, yyyy] = parts;
          const d = parseInt(dd, 10);
          const m = parseInt(mm, 10) - 1;
          const y = parseInt(yyyy, 10);
          if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
            const dt = new Date(Date.UTC(y, m, d));
            return toPKT_YMD(dt);
          }
        }
        const d2 = new Date(exp);
        if (!isNaN(d2.getTime())) return toPKT_YMD(d2);
        return null;
      }
      return null;
    };

    // Filter users whose expiry is TODAY or PAST (handles missed processing)
    const isDateLTE = (ymd, refY, refM, refD) => {
      if (ymd.y < refY) return true;
      if (ymd.y === refY && ymd.m < refM) return true;
      if (ymd.y === refY && ymd.m === refM && ymd.d <= refD) return true;
      return false;
    };

    const expiredUsers = usersAll
      .map(u => ({ u, ymd: parseExpiryYMD(u.expiryDate) }))
      .filter(({ ymd }) => ymd && isDateLTE(ymd, todayY, todayM, todayD))
      .map(({ u }) => u);
    
    console.log(`✅ Found ${expiredUsers.length} users with expiry TODAY (will move to Unpaid)`);
    
    if (expiredUsers.length > 0) {
      // Create voucher, change status to unpaid, and remove from Expiring Soon
      for (const user of expiredUsers) {
        console.log(`   - Processing ${user.userName} (expiry date reached: ${user.expiryDate})`);
        
        // Parse expiry date (DD-MM-YYYY or DD/MM/YYYY)
        const parseDate = (dateStr) => {
          const parts = dateStr.split(/[-\/]/);
          if (parts.length === 3) {
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1;
            const year = parseInt(parts[2], 10);
            return new Date(year, month, day);
          }
          return new Date(dateStr);
        };
        
        const currentExpiryDate = parseDate(user.expiryDate);
        
        // CRITICAL: Voucher should be for the CURRENT expiry month, not next month
        // Example: If expiry is 20 Nov, voucher should be for November (current expiry month)
        // The next expiry date is only for updating user's expiry date for next cycle
        const monthName = currentExpiryDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
        
        // Calculate next month's expiry date (for updating user's expiry date)
        const nextExpiryDate = new Date(currentExpiryDate);
        nextExpiryDate.setMonth(nextExpiryDate.getMonth() + 1);
        
        // Format dates as DD-MM-YYYY
        const formatDate = (date) => {
          const dd = String(date.getDate()).padStart(2, '0');
          const mm = String(date.getMonth() + 1).padStart(2, '0');
          const yyyy = date.getFullYear();
          return `${dd}-${mm}-${yyyy}`;
        };
        
        const newExpiryDateStr = formatDate(nextExpiryDate);
        
        console.log(`   → Creating voucher for current expiry month: ${monthName}`);
        console.log(`   → New expiry date for next cycle: ${newExpiryDateStr}`);
        
        // Update user: change to unpaid, update expiry date, remove from Expiring Soon
        await usersCollection.updateOne(
          { _id: user._id },
          { 
            $set: { 
              status: 'unpaid',
              expiryDate: newExpiryDateStr,
              showInExpiringSoon: false,
              unpaidSince: new Date()
            } 
          }
        );
        
        // Find or create voucher for this user
        let userVoucher = await vouchersCollection.findOne({ userId: user._id.toString() });
        
        const packageFeePerMonth = Number(user.amount || 0);
        const discountPerMonth = Number(user.discount || 0);
        const remainingAfterDiscount = Math.max(0, packageFeePerMonth - discountPerMonth);

        const newMonth = {
          month: monthName,
          packageFee: packageFeePerMonth,
          discount: discountPerMonth,
          paidAmount: 0,
          remainingAmount: remainingAfterDiscount,
          paymentMethod: 'Pending',
          receivedBy: '',
          paymentType: 'later',
          status: 'unpaid',
          description: `${monthName} - Pending Payment`,
          date: currentExpiryDate.toISOString(), // Use current expiry date for the month date
          createdAt: new Date()
        };
        
        if (userVoucher) {
          // Check if next month already exists
          const monthExists = userVoucher.months?.some(m => m.month === monthName);
          
          if (!monthExists) {
            // Add new unpaid month
            await vouchersCollection.updateOne(
              { userId: user._id.toString() },
              { 
                $push: { months: newMonth },
                $set: { 
                  expiryDate: newExpiryDateStr,
                  updatedAt: new Date()
                }
              }
            );
            console.log(`   ✅ Added ${monthName} voucher to existing record`);
          } else {
            console.log(`   ⚠️ ${monthName} voucher already exists`);
          }
        } else {
          // Create new voucher document
          const newVoucher = {
            userId: user._id.toString(),
            userName: user.userName,
            packageName: user.packageName,
            rechargeDate: user.rechargeDate,
            expiryDate: newExpiryDateStr,
            months: [newMonth],
            createdAt: new Date(),
            updatedAt: new Date()
          };
          
          await vouchersCollection.insertOne(newVoucher);
          console.log(`   ✅ Created new voucher with ${monthName}`);
        }
        
        console.log(`   ✅ ${user.userName} moved to UNPAID and removed from Expiring Soon`);
      }
      
      console.log(`✅ Successfully processed ${expiredUsers.length} users`);
    }
    
    // Check for users whose expiry date has passed (for Expired section)
    console.log('🕐 Checking for users whose expiry date has passed...');
    
    const expiredDate = new Date(today);
    expiredDate.setDate(expiredDate.getDate() - 1); // Yesterday or earlier
    
    // Find users whose expiry date has passed and are not inactive
    const pastExpiredUsers = await usersCollection.find({
      expiryDate: { $lt: today.toISOString() },
      serviceStatus: { $ne: 'inactive' }
    }).toArray();
    
    console.log(`✅ Found ${pastExpiredUsers.length} users with expired subscriptions`);
    
    // No need to update anything here as the expired users API endpoint will filter them automatically
    // This is just for logging purposes
    
  } catch (error) {
    console.error('❌ Error in moveTodayExpiredToUnpaid task:', error);
  }
};

// Initialize scheduled tasks (called after MongoDB connection)
const initializeScheduledTasks = () => {
  // ===== CRON DISABLED - Using External Cron Service (cron-job.org) =====
  // External cron hits these endpoints:
  // - https://techno-server-teal.vercel.app/api/admin/run-expiry-processing (12 PM daily)
  // - https://techno-server-teal.vercel.app/api/admin/run-reminders (8 PM daily)
  
  // IMPORTANT: Do NOT run processing on server start
  // Only external cron (cron-job.org) should trigger at scheduled times (12 PM)
  // This prevents premature expiry processing before 12 PM
  
  console.log('📅 Server started - Using external cron service (cron-job.org)');
  console.log('   → Expiry processing will run at 12 PM via cron-job.org');
  console.log('   → Endpoint: /api/admin/run-expiry-processing');
  console.log('   → Reminder processing endpoint: /api/admin/run-reminders');
};

// ============ ADMIN TRIGGERS (FOR EXTERNAL CRON SERVICE) ============
// Endpoint for expiry processing - called by cron-job.org at 12 PM daily
// Supports both GET (for cron-job.org) and POST (for manual triggers)
const handleExpiryProcessing = async (req, res) => {
  try {
    console.log('⚙️ Expiry Processing: Running now...');
    await moveTodayExpiredToUnpaid();
    await checkTomorrowExpiringUsers();
    res.status(200).json({
      success: true,
      message: 'Expiry processing executed successfully',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('❌ Expiry processing failed:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to run expiry processing',
      error: error.message
    });
  }
};

app.get('/api/admin/run-expiry-processing', ensureDbConnection, handleExpiryProcessing);
app.post('/api/admin/run-expiry-processing', ensureDbConnection, handleExpiryProcessing);

// Endpoint for reminder processing - called by cron-job.org at 8 PM daily
// Supports both GET (for cron-job.org) and POST (for manual triggers)
const handleReminderProcessing = async (req, res) => {
  try {
    console.log('⚙️ Reminder Processing: Running now...');
    await checkAndSendReminders();
    res.status(200).json({
      success: true,
      message: 'Reminder processing executed successfully',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('❌ Reminder processing failed:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to run reminder processing',
      error: error.message
    });
  }
};

app.get('/api/admin/run-reminders', ensureDbConnection, handleReminderProcessing);
app.post('/api/admin/run-reminders', ensureDbConnection, handleReminderProcessing);

// ============ PAYMENT REMINDER ENDPOINTS ============

// Create a payment reminder
app.post('/api/reminders', ensureDbConnection, async (req, res) => {
  try {
    const { userId, userName, amount, reminderDate, note } = req.body;

    if (!userId || !userName || !reminderDate) {
      return res.status(400).json({
        success: false,
        message: 'userId, userName, and reminderDate are required'
      });
    }

    // Create reminder object
    const reminder = {
      userId,
      userName,
      amount: amount || 0,
      reminderDate: new Date(reminderDate),
      note: note || '',
      notificationTime: '20:00', // 8 PM
      sent: false,
      createdAt: new Date()
    };

    const result = await remindersCollection.insertOne(reminder);

    console.log('✅ Reminder created:', { userId, userName, reminderDate });

    res.status(201).json({
      success: true,
      message: 'Reminder created successfully',
      data: { ...reminder, _id: result.insertedId }
    });
  } catch (error) {
    console.error('❌ Error creating reminder:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create reminder',
      error: error.message
    });
  }
});

// Get all active reminders (not sent yet)
app.get('/api/reminders', ensureDbConnection, async (req, res) => {
  try {
    const reminders = await remindersCollection
      .find({ sent: false })
      .sort({ reminderDate: 1 })
      .toArray();

    res.status(200).json({
      success: true,
      data: reminders
    });
  } catch (error) {
    console.error('❌ Error fetching reminders:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch reminders',
      error: error.message
    });
  }
});

// Get reminders for a specific user
app.get('/api/reminders/user/:userId', ensureDbConnection, async (req, res) => {
  try {
    const { userId } = req.params;

    const reminders = await remindersCollection
      .find({ userId, sent: false })
      .sort({ reminderDate: 1 })
      .toArray();

    res.status(200).json({
      success: true,
      data: reminders
    });
  } catch (error) {
    console.error('❌ Error fetching user reminders:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch user reminders',
      error: error.message
    });
  }
});

// Delete a reminder
app.delete('/api/reminders/:id', ensureDbConnection, async (req, res) => {
  try {
    const { id } = req.params;

    const result = await remindersCollection.deleteOne({
      _id: new ObjectId(id)
    });

    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Reminder not found'
      });
    }

    console.log('✅ Reminder deleted:', id);

    res.status(200).json({
      success: true,
      message: 'Reminder deleted successfully'
    });
  } catch (error) {
    console.error('❌ Error deleting reminder:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete reminder',
      error: error.message
    });
  }
});

// Function to check and send reminders at 8 PM
const checkAndSendReminders = async () => {
  try {
    console.log('🔔 Checking for reminders at 8:00 PM...');

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    // Find all reminders for today that haven't been sent
    const reminders = await remindersCollection.find({
      reminderDate: {
        $gte: today,
        $lt: tomorrow
      },
      sent: false
    }).toArray();

    console.log(`📋 Found ${reminders.length} reminder(s) for today`);

    if (reminders.length === 0) {
      return;
    }

    // Process each reminder
    for (const reminder of reminders) {
      console.log(`📬 Sending reminder notification for: ${reminder.userName}`);
      console.log(`   User ID: ${reminder.userId}`);
      console.log(`   Amount: Rs ${reminder.amount}`);
      console.log(`   Note: ${reminder.note || 'No note'}`);

      // Mark reminder as sent
      await remindersCollection.updateOne(
        { _id: reminder._id },
        {
          $set: {
            sent: true,
            sentAt: new Date()
          }
        }
      );

      // Here you can add actual notification logic:
      // - Push notification
      // - Email notification
      // - SMS notification
      // - Desktop notification
      
      // For now, we're just logging it
      console.log(`✅ Reminder marked as sent for ${reminder.userName}`);
    }

    console.log(`✅ Processed ${reminders.length} reminder(s) successfully`);
  } catch (error) {
    console.error('❌ Error checking reminders:', error);
  }
};

// Function to check and send missed reminders on server startup
const checkMissedReminders = async () => {
  try {
    console.log('🔍 Checking for missed reminders on server startup...');

    const now = new Date();
    const currentHour = now.getHours();

    // Get today's date at midnight
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    // If current time is past 8 PM, send today's reminders
    if (currentHour >= 20) {
      console.log('⏰ Current time is past 8:00 PM, sending today\'s reminders...');
      
      const todayReminders = await remindersCollection.find({
        reminderDate: {
          $gte: today,
          $lt: tomorrow
        },
        sent: false
      }).toArray();

      if (todayReminders.length > 0) {
        console.log(`📋 Found ${todayReminders.length} missed reminder(s) for today`);
        
        for (const reminder of todayReminders) {
          console.log(`📬 Sending missed reminder for: ${reminder.userName}`);
          console.log(`   User ID: ${reminder.userId}`);
          console.log(`   Amount: Rs ${reminder.amount}`);
          console.log(`   Note: ${reminder.note || 'No note'}`);

          await remindersCollection.updateOne(
            { _id: reminder._id },
            {
              $set: {
                sent: true,
                sentAt: now
              }
            }
          );

          console.log(`✅ Missed reminder sent for ${reminder.userName}`);
        }
      } else {
        console.log('✓ No missed reminders for today');
      }
    }

    // Also check for any past reminders that were never sent
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    const pastReminders = await remindersCollection.find({
      reminderDate: {
        $lt: today
      },
      sent: false
    }).toArray();

    if (pastReminders.length > 0) {
      console.log(`⚠️ Found ${pastReminders.length} old unsent reminder(s) from previous days`);
      
      for (const reminder of pastReminders) {
        const reminderDate = new Date(reminder.reminderDate);
        console.log(`📬 Sending overdue reminder for: ${reminder.userName}`);
        console.log(`   Original date: ${reminderDate.toLocaleDateString('en-GB')}`);
        console.log(`   User ID: ${reminder.userId}`);
        console.log(`   Amount: Rs ${reminder.amount}`);

        await remindersCollection.updateOne(
          { _id: reminder._id },
          {
            $set: {
              sent: true,
              sentAt: now
            }
          }
        );

        console.log(`✅ Overdue reminder sent for ${reminder.userName}`);
      }
    } else {
      console.log('✓ No overdue reminders found');
    }

    console.log('✅ Missed reminders check completed');
  } catch (error) {
    console.error('❌ Error checking missed reminders:', error);
  }
};

// ============ ROUTERS API ROUTES ============

// GET all routers
app.get('/api/routers', async (req, res) => {
  try {
    const routers = await routersCollection.find().sort({ createdAt: -1 }).toArray();
    res.status(200).json({
      success: true,
      count: routers.length,
      routers: routers
    });
  } catch (error) {
    console.error('Error fetching routers:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching routers',
      error: error.message
    });
  }
});

// POST create a new router
app.post('/api/routers', async (req, res) => {
  try {
    const { brand, model, quantity, price, purchasePrice, supplier, purchaseDate, status } = req.body;

    if (!brand || !model || quantity === undefined || price === undefined) {
      return res.status(400).json({
        success: false,
        message: 'Brand, model, quantity, and price are required'
      });
    }

    const router = {
      brand: brand.trim(),
      model: model.trim(),
      quantity: parseInt(quantity),
      quantitySold: 0,
      price: parseFloat(price),
      purchasePrice: purchasePrice ? parseFloat(purchasePrice) : undefined,
      supplier: supplier ? supplier.trim() : '',
      purchaseDate: purchaseDate || new Date().toISOString().split('T')[0],
      status: status || 'Available',
      salesHistory: [],
      createdAt: new Date()
    };

    const result = await routersCollection.insertOne(router);
    
    res.status(201).json({
      success: true,
      message: 'Router added successfully',
      data: {
        _id: result.insertedId,
        ...router
      }
    });
  } catch (error) {
    console.error('Error adding router:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding router',
      error: error.message
    });
  }
});

// UPDATE a router
app.put('/api/routers/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid router ID format'
      });
    }

    const { brand, model, quantity, availableStock, price, purchasePrice, supplier, purchaseDate, status } = req.body;

    const updateData = {};
    if (brand !== undefined) updateData.brand = brand.trim();
    if (model !== undefined) updateData.model = model.trim();
    // If availableStock is provided, use the total quantity from frontend
    // Otherwise use the quantity directly
    if (quantity !== undefined) updateData.quantity = parseInt(quantity);
    if (price !== undefined) updateData.price = parseFloat(price);
    if (purchasePrice !== undefined) updateData.purchasePrice = parseFloat(purchasePrice);
    if (supplier !== undefined) updateData.supplier = supplier.trim();
    if (purchaseDate !== undefined) updateData.purchaseDate = purchaseDate;
    if (status !== undefined) updateData.status = status;

    const result = await routersCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Router not found'
      });
    }

    res.json({
      success: true,
      message: 'Router updated successfully'
    });
  } catch (error) {
    console.error('Error updating router:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating router',
      error: error.message
    });
  }
});

// DELETE a router
app.delete('/api/routers/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid router ID format'
      });
    }

    const result = await routersCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Router not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Router deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting router:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting router',
      error: error.message
    });
  }
});

// POST sell router
app.post('/api/routers/:id/sell', async (req, res) => {
  try {
    const { quantity, sellingPrice, customerName, notes } = req.body;

    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid router ID format'
      });
    }

    if (!quantity || quantity <= 0) {
      return res.status(400).json({
        success: false,
        message: 'Valid quantity is required'
      });
    }

    const router = await routersCollection.findOne({ _id: new ObjectId(req.params.id) });
    
    if (!router) {
      return res.status(404).json({
        success: false,
        message: 'Router not found'
      });
    }

    const availableQty = router.quantity - (router.quantitySold || 0);
    if (quantity > availableQty) {
      return res.status(400).json({
        success: false,
        message: `Only ${availableQty} units available to sell`
      });
    }

    const saleRecord = {
      quantity: parseInt(quantity),
      sellingPrice: sellingPrice ? parseFloat(sellingPrice) : router.price,
      customerName: customerName || '',
      notes: notes || '',
      saleDate: new Date().toISOString()
    };

    const result = await routersCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      {
        $inc: { quantitySold: parseInt(quantity) },
        $push: { salesHistory: saleRecord }
      }
    );

    // Update monthly sales
    const now = new Date();
    const pktDate = new Date(now.getTime() + (PKT_OFFSET_MIN * 60000));
    const currentMonth = pktDate.getMonth() + 1;
    const currentYear = pktDate.getFullYear();
    const monthKey = `${currentYear}-${String(currentMonth).padStart(2, '0')}`;
    
    const saleAmount = parseInt(quantity) * parseFloat(saleRecord.sellingPrice);
    
    await monthlySalesCollection.updateOne(
      { monthKey: monthKey },
      {
        $inc: {
          totalSales: saleAmount,
          routerSales: saleAmount
        },
        $set: {
          month: currentMonth,
          year: currentYear,
          lastUpdated: new Date().toISOString()
        }
      },
      { upsert: true }
    );

    res.status(200).json({
      success: true,
      message: 'Router sold successfully',
      data: saleRecord
    });
  } catch (error) {
    console.error('Error selling router:', error);
    res.status(500).json({
      success: false,
      message: 'Error selling router',
      error: error.message
    });
  }
});

// GET sold routers
app.get('/api/routers/filter/sold', async (req, res) => {
  try {
    const routers = await routersCollection.find({
      quantitySold: { $gt: 0 }
    }).sort({ createdAt: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      count: routers.length,
      routers: routers
    });
  } catch (error) {
    console.error('Error fetching sold routers:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching sold routers',
      error: error.message
    });
  }
});

// GET router sales report by date range
app.get('/api/routers/sales-report', async (req, res) => {
  try {
    const { fromDate, toDate } = req.query;
    
    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: 'From date and to date are required'
      });
    }

    const from = new Date(fromDate);
    from.setHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setHours(23, 59, 59, 999);

    // Get all routers with sales history
    const routers = await routersCollection.find({
      'salesHistory.0': { $exists: true }
    }).toArray();

    let sales = [];
    let totalQuantitySold = 0;
    let totalRevenue = 0;
    let totalProfit = 0;

    routers.forEach(router => {
      if (router.salesHistory && router.salesHistory.length > 0) {
        router.salesHistory.forEach(sale => {
          const saleDate = new Date(sale.saleDate);
          if (saleDate >= from && saleDate <= to) {
            const revenue = sale.quantity * sale.sellingPrice;
            const purchasePrice = router.purchasePrice || 0;
            const profit = (sale.sellingPrice - purchasePrice) * sale.quantity;
            
            sales.push({
              brand: router.brand,
              model: router.model,
              quantity: sale.quantity,
              sellingPrice: sale.sellingPrice,
              customerName: sale.customerName,
              notes: sale.notes,
              saleDate: sale.saleDate,
              profit: profit
            });

            totalQuantitySold += sale.quantity;
            totalRevenue += revenue;
            totalProfit += profit;
          }
        });
      }
    });

    // Sort sales by date (newest first)
    sales.sort((a, b) => new Date(b.saleDate) - new Date(a.saleDate));

    res.status(200).json({
      success: true,
      data: {
        sales,
        totalSales: sales.length,
        totalQuantitySold,
        totalRevenue,
        totalProfit
      }
    });
  } catch (error) {
    console.error('Error fetching router sales report:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching router sales report',
      error: error.message
    });
  }
});

// ============ FIBER CABLES API ROUTES ============

// GET all fiber cables
app.get('/api/fiber-cables', async (req, res) => {
  try {
    const cables = await fiberCablesCollection.find().sort({ createdAt: -1 }).toArray();
    res.status(200).json({
      success: true,
      count: cables.length,
      cables: cables
    });
  } catch (error) {
    console.error('Error fetching fiber cables:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching fiber cables',
      error: error.message
    });
  }
});

// POST create a new fiber cable
app.post('/api/fiber-cables', async (req, res) => {
  try {
    const { type, length, pricePerMeter, purchasePricePerMeter, purchaseDate, supplier, status } = req.body;

    if (!type || length === undefined || pricePerMeter === undefined) {
      return res.status(400).json({
        success: false,
        message: 'Type, length, and price per meter are required'
      });
    }

    const cable = {
      type: type.trim(),
      length: parseFloat(length),
      lengthSold: 0,
      pricePerMeter: parseFloat(pricePerMeter),
      purchasePricePerMeter: purchasePricePerMeter ? parseFloat(purchasePricePerMeter) : undefined,
      purchaseDate: purchaseDate || new Date().toISOString().split('T')[0],
      supplier: supplier ? supplier.trim() : '',
      status: status || 'Available',
      salesHistory: [],
      createdAt: new Date()
    };

    const result = await fiberCablesCollection.insertOne(cable);
    
    res.status(201).json({
      success: true,
      message: 'Fiber cable added successfully',
      data: {
        _id: result.insertedId,
        ...cable
      }
    });
  } catch (error) {
    console.error('Error adding fiber cable:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding fiber cable',
      error: error.message
    });
  }
});

// UPDATE a fiber cable
app.put('/api/fiber-cables/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid fiber cable ID format'
      });
    }

    const { type, length, availableStock, pricePerMeter, purchasePricePerMeter, purchaseDate, supplier, status } = req.body;

    const updateData = {};
    if (type !== undefined) updateData.type = type.trim();
    // If availableStock is provided, use the total length from frontend
    // Otherwise use the length directly
    if (length !== undefined) updateData.length = parseFloat(length);
    if (pricePerMeter !== undefined) updateData.pricePerMeter = parseFloat(pricePerMeter);
    if (purchasePricePerMeter !== undefined) updateData.purchasePricePerMeter = parseFloat(purchasePricePerMeter);
    if (purchaseDate !== undefined) updateData.purchaseDate = purchaseDate;
    if (supplier !== undefined) updateData.supplier = supplier.trim();
    if (status !== undefined) updateData.status = status;

    const result = await fiberCablesCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fiber cable not found'
      });
    }

    res.json({
      success: true,
      message: 'Fiber cable updated successfully'
    });
  } catch (error) {
    console.error('Error updating fiber cable:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating fiber cable',
      error: error.message
    });
  }
});

// DELETE a fiber cable
app.delete('/api/fiber-cables/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid fiber cable ID format'
      });
    }

    const result = await fiberCablesCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fiber cable not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Fiber cable deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting fiber cable:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting fiber cable',
      error: error.message
    });
  }
});

// POST sell fiber cable
app.post('/api/fiber-cables/:id/sell', async (req, res) => {
  try {
    const { length, sellingPricePerMeter, customerName, notes } = req.body;

    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid fiber cable ID format'
      });
    }

    if (!length || length <= 0) {
      return res.status(400).json({
        success: false,
        message: 'Valid length is required'
      });
    }

    const cable = await fiberCablesCollection.findOne({ _id: new ObjectId(req.params.id) });
    
    if (!cable) {
      return res.status(404).json({
        success: false,
        message: 'Fiber cable not found'
      });
    }

    const availableLength = cable.length - (cable.lengthSold || 0);
    if (length > availableLength) {
      return res.status(400).json({
        success: false,
        message: `Only ${availableLength} meters available to sell`
      });
    }

    const saleRecord = {
      length: parseFloat(length),
      sellingPricePerMeter: sellingPricePerMeter ? parseFloat(sellingPricePerMeter) : cable.pricePerMeter,
      customerName: customerName || '',
      notes: notes || '',
      saleDate: new Date().toISOString()
    };

    const result = await fiberCablesCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      {
        $inc: { lengthSold: parseFloat(length) },
        $push: { salesHistory: saleRecord }
      }
    );

    // Update monthly sales
    const now = new Date();
    const pktDate = new Date(now.getTime() + (PKT_OFFSET_MIN * 60000));
    const currentMonth = pktDate.getMonth() + 1;
    const currentYear = pktDate.getFullYear();
    const monthKey = `${currentYear}-${String(currentMonth).padStart(2, '0')}`;
    
    const saleAmount = parseFloat(length) * parseFloat(saleRecord.sellingPricePerMeter);
    
    await monthlySalesCollection.updateOne(
      { monthKey: monthKey },
      {
        $inc: {
          totalSales: saleAmount,
          cableSales: saleAmount
        },
        $set: {
          month: currentMonth,
          year: currentYear,
          lastUpdated: new Date().toISOString()
        }
      },
      { upsert: true }
    );

    res.status(200).json({
      success: true,
      message: 'Fiber cable sold successfully',
      data: saleRecord
    });
  } catch (error) {
    console.error('Error selling fiber cable:', error);
    res.status(500).json({
      success: false,
      message: 'Error selling fiber cable',
      error: error.message
    });
  }
});

// GET sold fiber cables
app.get('/api/fiber-cables/filter/sold', async (req, res) => {
  try {
    const cables = await fiberCablesCollection.find({
      lengthSold: { $gt: 0 }
    }).sort({ createdAt: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      count: cables.length,
      cables: cables
    });
  } catch (error) {
    console.error('Error fetching sold fiber cables:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching sold fiber cables',
      error: error.message
    });
  }
});

// GET fiber cable sales report by date range
app.get('/api/fiber-cables/sales-report', async (req, res) => {
  try {
    const { fromDate, toDate } = req.query;
    
    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: 'From date and to date are required'
      });
    }

    const from = new Date(fromDate);
    from.setHours(0, 0, 0, 0);
    const to = new Date(toDate);
    to.setHours(23, 59, 59, 999);

    // Get all fiber cables with sales history
    const cables = await fiberCablesCollection.find({
      'salesHistory.0': { $exists: true }
    }).toArray();

    let sales = [];
    let totalLengthSold = 0;
    let totalRevenue = 0;
    let totalProfit = 0;

    cables.forEach(cable => {
      if (cable.salesHistory && cable.salesHistory.length > 0) {
        cable.salesHistory.forEach(sale => {
          const saleDate = new Date(sale.saleDate);
          if (saleDate >= from && saleDate <= to) {
            const revenue = sale.length * sale.sellingPrice;
            const purchasePricePerMeter = cable.purchasePricePerMeter || 0;
            const profit = (sale.sellingPrice - purchasePricePerMeter) * sale.length;
            
            sales.push({
              type: cable.type,
              length: sale.length,
              sellingPrice: sale.sellingPrice,
              customerName: sale.customerName,
              notes: sale.notes,
              saleDate: sale.saleDate,
              profit: profit
            });

            totalLengthSold += sale.length;
            totalRevenue += revenue;
            totalProfit += profit;
          }
        });
      }
    });

    // Sort sales by date (newest first)
    sales.sort((a, b) => new Date(b.saleDate) - new Date(a.saleDate));

    res.status(200).json({
      success: true,
      data: {
        sales,
        totalSales: sales.length,
        totalLengthSold,
        totalRevenue,
        totalProfit
      }
    });
  } catch (error) {
    console.error('Error fetching fiber cable sales report:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching fiber cable sales report',
      error: error.message
    });
  }
});

// ============ TOTAL SALES API ROUTE ============

// GET total sales from routers and fiber cables
// Get current month's total sales
app.get('/api/sales/total', async (req, res) => {
  try {
    // Get current month and year in PKT
    const now = new Date();
    const pktDate = new Date(now.getTime() + (PKT_OFFSET_MIN * 60000));
    const currentMonth = pktDate.getMonth() + 1; // 1-12
    const currentYear = pktDate.getFullYear();
    const monthKey = `${currentYear}-${String(currentMonth).padStart(2, '0')}`; // e.g., "2024-12"
    
    // Get monthly sales record
    const monthlySales = await monthlySalesCollection.findOne({
      monthKey: monthKey
    });
    
    const totalSales = monthlySales ? monthlySales.totalSales : 0;
    const routerSales = monthlySales ? monthlySales.routerSales : 0;
    const cableSales = monthlySales ? monthlySales.cableSales : 0;
    
    res.status(200).json({
      success: true,
      data: {
        totalSales,
        routerSales,
        cableSales,
        breakdown: {
          routers: routerSales,
          fiberCables: cableSales
        },
        month: monthKey
      }
    });
  } catch (error) {
    console.error('Error fetching total sales:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching total sales',
      error: error.message
    });
  }
});

// Get sales history by month
app.get('/api/sales/history', async (req, res) => {
  try {
    const history = await monthlySalesCollection.find({})
      .sort({ year: -1, month: -1 })
      .limit(12)
      .toArray();
    
    res.status(200).json({
      success: true,
      data: history
    });
  } catch (error) {
    console.error('Error fetching sales history:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching sales history',
      error: error.message
    });
  }
});

// ============ VOUCHERS ROUTES ============
// Get all vouchers (or filter by userId if query parameter provided)
app.get('/api/vouchers', ensureDbConnection, async (req, res) => {
  try {
    const { userId } = req.query;
    
    // If userId query parameter is provided, filter by userId
    const query = userId ? { userId } : {};
    const vouchers = await vouchersCollection.find(query).toArray();
    
    res.status(200).json({
      success: true,
      data: vouchers
    });
  } catch (error) {
    console.error('Error fetching vouchers:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching vouchers',
      error: error.message
    });
  }
});

// Get vouchers by user ID
app.get('/api/vouchers/user/:userId', ensureDbConnection, async (req, res) => {
  try {
    const { userId } = req.params;
    const vouchers = await vouchersCollection.find({ userId }).toArray();
    
    // Debug: Log receivedBy for first voucher's first month
    if (vouchers.length > 0 && vouchers[0].months && vouchers[0].months.length > 0) {
      const firstMonth = vouchers[0].months[0];
      console.log(`🔍 Voucher API - First month receivedBy:`, {
        month: firstMonth.month,
        receivedBy: firstMonth.receivedBy,
        hasPaymentHistory: !!firstMonth.paymentHistory,
        paymentHistoryReceivedBy: firstMonth.paymentHistory?.map((p) => p.receivedBy).filter(Boolean) || []
      });
    }
    
    res.status(200).json({
      success: true,
      data: vouchers
    });
  } catch (error) {
    console.error('Error fetching user vouchers:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching user vouchers',
      error: error.message
    });
  }
});

// PUT update voucher
app.put('/api/vouchers/:id', ensureDbConnection, async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid voucher ID format'
      });
    }

    const updateData = req.body;
    
    // Check if this is a refund operation (has reversed months)
    if (updateData.months && updateData.isRefund) {
      console.log('🔄 REFUND DETECTED');
      
      // Get the voucher to access user info
      const voucher = await vouchersCollection.findOne({ _id: new ObjectId(req.params.id) });
      
      if (!voucher) {
        return res.status(404).json({
          success: false,
          message: 'Voucher not found'
        });
      }
      
      // Get user details
      const user = await usersCollection.findOne({ _id: new ObjectId(voucher.userId) });
      
      // Find months that were set to reversed
      const reversedMonths = updateData.months.filter(m => m.status === 'reversed');
      
      if (reversedMonths.length > 0) {
        // Create refund collection entry
        const refundsCollection = db.collection('refunds');
        const refundRecord = {
          userId: voucher.userId,
          voucherId: voucher._id.toString(),
          userName: user?.userName || 'Unknown',
          packageName: user?.packageName || 'Unknown',
          refundedMonths: reversedMonths.map(m => ({
            month: m.month,
            packageFee: m.packageFee,
            discount: m.discount || 0,
            refundedAmount: m.refundedAmount || m.paidAmount || 0,
            remainingAmount: m.remainingAmount,
            refundDate: m.refundDate || new Date().toISOString()
          })),
          status: 'reversed',
          createdAt: new Date(),
          totalRefundedAmount: reversedMonths.reduce((sum, m) => sum + (m.refundedAmount || m.paidAmount || 0), 0)
        };
        
        const insertResult = await refundsCollection.insertOne(refundRecord);
        console.log(`✅ REFUND SAVED: ${user?.userName} - ${reversedMonths.length} months - ID: ${insertResult.insertedId}`);
        
        // Check if ALL months are now reversed
        const totalMonths = updateData.months.length;
        const allReversed = updateData.months.every(m => m.status === 'reversed');
        
        if (allReversed) {
          console.log(`🔄 ALL ${totalMonths} months reversed - removing user from unpaid list`);
          // Change user status from 'unpaid' to 'reversed' so they don't appear in unpaid section
          await usersCollection.updateOne(
            { _id: new ObjectId(voucher.userId) },
            { $set: { status: 'reversed' } }
          );
          console.log(`✅ User ${user?.userName} marked as 'reversed' (excluded from unpaid)`);
        } else {
          console.log(`⚠️ Partial refund: ${reversedMonths.length}/${totalMonths} months reversed - user stays in unpaid`);
        }
      }
    }
    
    // Remove isRefund flag before updating voucher
    delete updateData.isRefund;
    
    // CRITICAL: Sort months by date (FIFO - First In First Out) before storing
    // This ensures months are always stored in chronological order (earliest first)
    if (updateData.months && Array.isArray(updateData.months)) {
      const parseDate = (dateStr) => {
        if (!dateStr) return new Date(0);
        if (dateStr instanceof Date) return dateStr;
        if (typeof dateStr === 'string') {
          // Check if ISO format (YYYY-MM-DD or full ISO string)
          if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
            // ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss.sssZ
            const parsed = new Date(dateStr);
            if (!isNaN(parsed.getTime())) return parsed;
          }
          
          // Try DD-MM-YYYY format
          const parts = dateStr.split('-');
          if (parts.length === 3 && parts[2].length === 4) {
            // DD-MM-YYYY format (last part is year with 4 digits)
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1;
            const year = parseInt(parts[2], 10);
            if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
              return new Date(year, month, day);
            }
          }
          
          // Fallback to standard Date parsing
          const parsed = new Date(dateStr);
          return isNaN(parsed.getTime()) ? new Date(0) : parsed;
        }
        return new Date(0);
      };
      
      const sortedMonths = [...updateData.months].sort((a, b) => {
        const dateA = parseDate(a.date || a.createdAt);
        const dateB = parseDate(b.date || b.createdAt);
        return dateA.getTime() - dateB.getTime(); // Ascending: earliest first
      });
      
      console.log(`📅 Backend PUT (with refund): Months sorted by date (FIFO order):`, sortedMonths.map(m => `${m.month} (${m.date || m.createdAt})`).join(', '));
      updateData.months = sortedMonths;
    }
    
    const result = await vouchersCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Voucher not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Voucher updated successfully'
    });
  } catch (error) {
    console.error('Error updating voucher:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating voucher',
      error: error.message
    });
  }
});

// POST create refund
app.post('/api/refunds', ensureDbConnection, async (req, res) => {
  try {
    const refundsCollection = db.collection('refunds');
    const refundData = req.body;
    
    const result = await refundsCollection.insertOne({
      ...refundData,
      createdAt: new Date()
    });
    
    console.log(`✅ REFUND SAVED: ${refundData.userName} - ${refundData.refundedMonths?.length || 0} months`);
    
    res.status(201).json({
      success: true,
      data: { _id: result.insertedId, ...refundData }
    });
  } catch (error) {
    console.error('Error creating refund:', error);
    res.status(500).json({
      success: false,
      message: 'Error creating refund',
      error: error.message
    });
  }
});

// GET refunds for a user
app.get('/api/refunds/:userId', ensureDbConnection, async (req, res) => {
  try {
    const userId = req.params.userId;
    const refundsCollection = db.collection('refunds');
    
    // Find all refunds for this user
    const refunds = await refundsCollection.find({ userId }).toArray();
    
    console.log(`📋 Found ${refunds.length} refund records for user ${userId}`);
    
    res.status(200).json({
      success: true,
      data: refunds
    });
  } catch (error) {
    console.error('Error fetching refunds:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching refunds',
      error: error.message
    });
  }
});

// POST process reversed payment - convert refund back to paid
app.post('/api/refunds/process-payment', ensureDbConnection, async (req, res) => {
  try {
    const { userId, notes } = req.body;
    
    if (!userId) {
      return res.status(400).json({
        success: false,
        message: 'userId is required'
      });
    }
    
    console.log(`💰 Processing reversed payment for user: ${userId}`);
    console.log(`📝 Notes: ${notes || 'No notes'}`);
    
    const refundsCollection = db.collection('refunds');
    
    // Find all refunds for this user
    const refunds = await refundsCollection.find({ userId }).toArray();
    
    if (refunds.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'No refund records found for this user'
      });
    }
    
    console.log(`📋 Found ${refunds.length} refund record(s)`);
    
    let processedMonths = 0;
    let totalAmount = 0;
    
    // Process each refund
    for (const refund of refunds) {
      const voucherId = refund.voucherId;
      console.log(`🔍 Processing refund with voucherId: ${voucherId}`);
      
      // Find the voucher by _id
      const voucher = await vouchersCollection.findOne({ _id: new ObjectId(voucherId) });
      
      if (!voucher) {
        console.log(`⚠️ Voucher not found: ${voucherId}`);
        continue;
      }
      
      console.log(`✅ Found voucher for user: ${voucher.userName}`);
      
      // Process each refunded month
      if (refund.refundedMonths && Array.isArray(refund.refundedMonths)) {
        for (const refundedMonth of refund.refundedMonths) {
          console.log(`📅 Processing month: ${refundedMonth.month}`);
          
          // Find matching month in voucher
          const monthIndex = voucher.months.findIndex(m => m.month === refundedMonth.month);
          
          if (monthIndex !== -1) {
            // Update month status to 'paid'
            await vouchersCollection.updateOne(
              { 
                _id: new ObjectId(voucherId),
                'months.month': refundedMonth.month
              },
              {
                $set: {
                  'months.$.status': 'paid',
                  'months.$.paidAmount': refundedMonth.packageFee - (refundedMonth.discount || 0),
                  'months.$.remainingAmount': 0,
                  'months.$.paymentMethod': 'Cash',
                  'months.$.receivedBy': 'Admin',
                  'months.$.description': `${refundedMonth.month} - Reversed payment processed${notes ? ': ' + notes : ''}`
                },
                $unset: {
                  'months.$.refundDate': '',
                  'months.$.refundedAmount': ''
                }
              }
            );
            
            processedMonths++;
            totalAmount += (refundedMonth.packageFee - (refundedMonth.discount || 0));
            console.log(`✅ Updated ${refundedMonth.month} to paid status`);
          } else {
            console.log(`⚠️ Month not found in voucher: ${refundedMonth.month}`);
          }
        }
      }
      
      // Remove the refund record
      await refundsCollection.deleteOne({ _id: refund._id });
      console.log(`🗑️ Removed refund record`);
    }
    
    // Update user status and amounts
    const user = await usersCollection.findOne({ _id: new ObjectId(userId) });
    if (user) {
      // Recalculate amounts from vouchers (excluding reversed months)
      const userVouchers = await vouchersCollection.find({ userId }).toArray();
      let totalPaid = 0;
      let totalRemaining = 0;
      let hasUnpaidMonths = false;
      
      for (const v of userVouchers) {
        if (v.months && Array.isArray(v.months)) {
          // Only count non-reversed months
          const nonReversedMonths = v.months.filter(m => {
            const isReversed = !!(m.refundDate || m.refundedAmount);
            return !isReversed;
          });
          
          nonReversedMonths.forEach(m => {
            totalPaid += Number(m.paidAmount || 0);
            totalRemaining += Number(m.remainingAmount || 0);
          });
          
          // Check if there are any remaining unpaid months
          if (nonReversedMonths.some(m => m.status === 'unpaid' || (m.remainingAmount && m.remainingAmount > 0))) {
            hasUnpaidMonths = true;
          }
        }
      }
      
      // Determine new status
      let newStatus = 'unpaid';
      if (totalRemaining === 0 && totalPaid > 0) {
        newStatus = 'paid';
      } else if (totalPaid > 0) {
        newStatus = 'partial';
      }
      
      // Update user with new amounts and status
      await usersCollection.updateOne(
        { _id: new ObjectId(userId) },
        { 
          $set: { 
            status: newStatus,
            paidAmount: totalPaid,
            remainingAmount: totalRemaining
          } 
        }
      );
      console.log(`✅ Updated user status to '${newStatus}', paidAmount: ${totalPaid}, remainingAmount: ${totalRemaining}`);
    }
    
    res.status(200).json({
      success: true,
      message: `Successfully processed ${processedMonths} reversed payment(s)`,
      data: {
        processedMonths,
        totalAmount,
        notes
      }
    });
  } catch (error) {
    console.error('❌ Error processing reversed payment:', error);
    res.status(500).json({
      success: false,
      message: 'Error processing reversed payment',
      error: error.message
    });
  }
});

// POST convert paid/partial month to unpaid (clear payment history)
app.post('/api/vouchers/convert-to-unpaid', ensureDbConnection, async (req, res) => {
  try {
    const { userId, month } = req.body;
    
    if (!userId || !month) {
      return res.status(400).json({
        success: false,
        message: 'userId and month are required'
      });
    }
    
    console.log(`🔄 Converting month ${month} to unpaid for user ${userId}`);
    
    // Find voucher for this user
    const voucher = await vouchersCollection.findOne({ userId });
    
    if (!voucher) {
      return res.status(404).json({
        success: false,
        message: 'Voucher not found for this user'
      });
    }
    
    if (!Array.isArray(voucher.months)) {
      return res.status(404).json({
        success: false,
        message: 'Month not found in voucher'
      });
    }
    
    // Find the month to convert
    const monthIndex = voucher.months.findIndex((m) => m.month === month);
    if (monthIndex === -1) {
      return res.status(404).json({
        success: false,
        message: 'Month not found in voucher'
      });
    }
    
    const monthData = voucher.months[monthIndex];
    const packageFee = Number(monthData.packageFee || 0);
    const discount = Number(monthData.discount || 0);
    const fullAmount = packageFee - discount;
    const refundedAmount = Number(monthData.paidAmount || 0);
    const receivedBy = monthData.receivedBy || '';
    const paymentMethod = monthData.paymentMethod || 'Cash';
    
    console.log(`💰 Reversing payment - Amount: Rs ${refundedAmount}, ReceivedBy: ${receivedBy}, Method: ${paymentMethod}`);
    
    // CRITICAL: Deduct from receiver's income if amount was paid
    if (refundedAmount > 0 && receivedBy) {
      try {
        // Determine which income field to deduct from based on payment method
        const isBankTransfer = paymentMethod.toLowerCase().includes('bank');
        const incomeField = isBankTransfer ? 'bankIncome' : 'cashIncome';
        
        console.log(`💳 Deducting Rs ${refundedAmount} from ${receivedBy}'s ${incomeField}`);
        
        // Get current income record
        const incomeRecord = await incomesCollection.findOne({ name: receivedBy });
        
        if (incomeRecord) {
          const currentAmount = Number(incomeRecord[incomeField] || 0);
          const newAmount = Math.max(0, currentAmount - refundedAmount); // Don't go below 0
          
          await incomesCollection.updateOne(
            { name: receivedBy },
            { 
              $set: { 
                [incomeField]: newAmount,
                updatedAt: new Date()
              } 
            }
          );
          
          console.log(`✅ Deducted from ${receivedBy}: ${currentAmount} - ${refundedAmount} = ${newAmount} (${incomeField})`);
        } else {
          console.log(`⚠️ No income record found for ${receivedBy} - skipping income deduction`);
        }
      } catch (incomeError) {
        console.error('❌ Error deducting from income:', incomeError);
        // Continue with reversal even if income deduction fails
      }
    }
    
    // Update month: convert to unpaid, clear payment history
    const updatedMonths = [...voucher.months];
    updatedMonths[monthIndex] = {
      ...monthData,
      status: 'unpaid',
      paidAmount: 0,
      remainingAmount: fullAmount,
      paymentMethod: '',
      receivedBy: '',
      paymentHistory: [],
      refundDate: new Date(),
      refundedAmount: refundedAmount
    };
    
    // Update voucher
    await vouchersCollection.updateOne(
      { _id: voucher._id },
      { $set: { months: updatedMonths } }
    );
    
    console.log(`✅ Updated month ${month} to unpaid status`);
    
    // Recalculate user totals
    const nonReversedMonths = updatedMonths.filter((m) => {
      const isReversed = !!(m.refundDate || m.refundedAmount);
      return !isReversed;
    });
    
    const totalPaid = nonReversedMonths.reduce((sum, m) => sum + (m.paidAmount || 0), 0);
    const totalRemaining = nonReversedMonths.reduce((sum, m) => sum + (m.remainingAmount || 0), 0);
    
    // CRITICAL: Check if ANY month is unpaid
    const hasUnpaidMonth = nonReversedMonths.some((m) => m.status === 'unpaid');
    
    // Determine user status
    // CRITICAL: If ANY month is unpaid (including the reversed one), status should be 'unpaid'
    let newStatus = 'unpaid';
    if (hasUnpaidMonth) {
      // If any month is unpaid, user status = 'unpaid' (show in Unpaid tab)
      newStatus = 'unpaid';
      console.log(`📊 Status: UNPAID (has ${nonReversedMonths.filter(m => m.status === 'unpaid').length} unpaid month(s))`);
    } else if (totalRemaining === 0 && totalPaid > 0) {
      // All months are fully paid
      newStatus = 'paid';
      console.log(`📊 Status: PAID (all months paid)`);
    } else if (totalPaid > 0) {
      // All months are partial (no unpaid months)
      newStatus = 'partial';
      console.log(`📊 Status: PARTIAL (all months partial, no unpaid)`);
    } else {
      // No payments made
      newStatus = 'unpaid';
      console.log(`📊 Status: UNPAID (no payments)`);
    }
    
    // Update user
    await usersCollection.updateOne(
      { _id: new ObjectId(userId) },
      {
        $set: {
          status: newStatus,
          paymentStatus: newStatus,
          paidAmount: totalPaid,
          remainingAmount: totalRemaining
        }
      }
    );
    
    console.log(`✅ Updated user status: ${newStatus}, paid: ${totalPaid}, remaining: ${totalRemaining}`);
    
    res.status(200).json({
      success: true,
      message: `Month ${month} successfully converted to unpaid`,
      data: {
        month,
        status: 'unpaid',
        remainingAmount: fullAmount
      }
    });
  } catch (error) {
    console.error('Error converting month to unpaid:', error);
    res.status(500).json({
      success: false,
      message: 'Error converting month to unpaid',
      error: error.message
    });
  }
});

// DELETE voucher
app.delete('/api/vouchers/:id', ensureDbConnection, async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid voucher ID format'
      });
    }

    const result = await vouchersCollection.deleteOne({ 
      _id: new ObjectId(req.params.id) 
    });

    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Voucher not found'
      });
    }

    res.status(200).json({
      success: true,
      message: 'Voucher deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting voucher:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting voucher',
      error: error.message
    });
  }
});

// ============ TRANSACTIONS ROUTES ============
// Get all transactions
app.get('/api/transactions', ensureDbConnection, async (req, res) => {
  try {
    const transactions = await transactionsCollection.find({}).toArray();
    res.status(200).json({
      success: true,
      data: transactions
    });
  } catch (error) {
    console.error('Error fetching transactions:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching transactions',
      error: error.message
    });
  }
});

// Create new transaction
app.post('/api/transactions', ensureDbConnection, async (req, res) => {
  try {
    const transactionData = req.body;
    const result = await transactionsCollection.insertOne(transactionData);
    res.status(201).json({
      success: true,
      data: { _id: result.insertedId, ...transactionData }
    });
  } catch (error) {
    console.error('Error creating transaction:', error);
    res.status(500).json({
      success: false,
      message: 'Error creating transaction',
      error: error.message
    });
  }
});

// ============ MIGRATION ENDPOINT ============
// One-time migration to add serviceStatus field to existing users
app.post('/api/migrate/add-service-status', async (req, res) => {
  try {
    console.log('🔄 Starting migration: Adding serviceStatus to existing users...');
    
    // Update all users without serviceStatus field
    const result = await usersCollection.updateMany(
      { serviceStatus: { $exists: false } }, // Only users without serviceStatus
      { $set: { serviceStatus: 'active' } }  // Set default to 'active'
    );
    
    console.log(`✅ Migration complete: ${result.modifiedCount} users updated`);
    
    res.status(200).json({
      success: true,
      message: 'Migration completed successfully',
      modifiedCount: result.modifiedCount
    });
  } catch (error) {
    console.error('❌ Migration failed:', error);
    res.status(500).json({
      success: false,
      message: 'Migration failed',
      error: error.message
    });
  }
});


// ============ NOTIFICATIONS API ROUTES ============

// GET route to fetch all notifications for admin
app.get('/api/notifications', ensureDbConnection, async (req, res) => {
  try {
    const { isRead } = req.query;
    
    let query = {};
    
    // Filter by read status if provided
    if (isRead !== undefined) {
      query.isRead = isRead === 'true';
    }
    
    const notifications = await notificationsCollection
      .find(query)
      .sort({ createdAt: -1 })
      .toArray();
    
    res.status(200).json({
      success: true,
      count: notifications.length,
      data: notifications
    });
  } catch (error) {
    console.error('Error fetching notifications:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching notifications',
      error: error.message
    });
  }
});

// PUT route to mark notification as read
app.put('/api/notifications/:id/read', ensureDbConnection, async (req, res) => {
  try {
    const notificationId = req.params.id;
    
    if (!ObjectId.isValid(notificationId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid notification ID format'
      });
    }
    
    const result = await notificationsCollection.updateOne(
      { _id: new ObjectId(notificationId) },
      { $set: { isRead: true, readAt: new Date() } }
    );
    
    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Notification not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Notification marked as read'
    });
  } catch (error) {
    console.error('Error marking notification as read:', error);
    res.status(500).json({
      success: false,
      message: 'Error marking notification as read',
      error: error.message
    });
  }
});

// PUT route to mark all notifications as read
app.put('/api/notifications/read-all', ensureDbConnection, async (req, res) => {
  try {
    const result = await notificationsCollection.updateMany(
      { isRead: false },
      { $set: { isRead: true, readAt: new Date() } }
    );
    
    res.status(200).json({
      success: true,
      message: 'All notifications marked as read',
      modifiedCount: result.modifiedCount
    });
  } catch (error) {
    console.error('Error marking all notifications as read:', error);
    res.status(500).json({
      success: false,
      message: 'Error marking all notifications as read',
      error: error.message
    });
  }
});

// DELETE route to delete a notification
app.delete('/api/notifications/:id', ensureDbConnection, async (req, res) => {
  try {
    const notificationId = req.params.id;
    
    if (!ObjectId.isValid(notificationId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid notification ID format'
      });
    }
    
    const result = await notificationsCollection.deleteOne(
      { _id: new ObjectId(notificationId) }
    );
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Notification not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'Notification deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting notification:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting notification',
      error: error.message
    });
  }
});

// PUT route to reopen a resolved complaint (Admin only)
app.put('/api/complaints/:id/reopen', ensureDbConnection, async (req, res) => {
  try {
    console.log('?? Reopen complaint request:', req.params.id);
    
    const complaintId = req.params.id;
    
    if (!complaintId) {
      return res.status(400).json({
        success: false,
        message: 'Complaint ID is required'
      });
    }
    
    // Validate ObjectId format
    if (!ObjectId.isValid(complaintId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid complaint ID format'
      });
    }

    // Get complaint first to check current status
    const complaint = await complaintsCollection.findOne(
      { _id: new ObjectId(complaintId) }
    );

    if (!complaint) {
      return res.status(404).json({
        success: false,
        message: 'Complaint not found'
      });
    }

    // Check if complaint is already pending
    if ((complaint.status || 'pending').toLowerCase() === 'pending') {
      return res.status(400).json({
        success: false,
        message: 'Complaint is already pending'
      });
    }

    // Reopen complaint by changing status to pending
    const result = await complaintsCollection.updateOne(
      { _id: new ObjectId(complaintId) },
      { 
        $set: { 
          status: 'pending',
          reopenedAt: new Date()
        } 
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Complaint not found'
      });
    }

    console.log('? Complaint reopened successfully');
    res.status(200).json({
      success: true,
      message: 'Complaint reopened successfully',
      data: {
        _id: complaintId,
        status: 'pending',
        reopenedAt: new Date()
      }
    });

  } catch (error) {
    console.error('? Error reopening complaint:', error);
    res.status(500).json({
      success: false,
      message: 'Error reopening complaint',
      error: error.message
    });
  }
});

// Start server (only in development, not in Vercel)
if (!process.env.VERCEL) {
  const PORT = process.env.PORT || 5000;
  const HOST = '0.0.0.0'; // Listen on all network interfaces

  // Connect to database on startup for local development
  connectToDatabase()
    .then(() => {
      app.listen(PORT, HOST, () => {
        console.log(`Server is running on http://${HOST}:${PORT}`);
        console.log(`Access from local network using your IP address`);
      });
    })
    .catch(err => {
      console.error('Failed to start server:', err);
      process.exit(1);
    });
} else {
  console.log('Running in serverless mode (Vercel)');
}

// ============ DEBUG ENDPOINT ============
// Check if specific user should be expired
app.get('/api/admin/check-user-expiry/:userId', ensureDbConnection, async (req, res) => {
  try {
    const user = await usersCollection.findOne({ _id: new ObjectId(req.params.userId) });
    
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }

    // Parse user's expiry date
    const parseDate = (dateStr) => {
      const parts = dateStr.split(/[-\/]/);
      if (parts.length === 3) {
        const day = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10) - 1;
        const year = parseInt(parts[2], 10);
        return new Date(year, month, day);
      }
      return new Date(dateStr);
    };

    const userExpiryDate = parseDate(user.expiryDate);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    // PKT timezone
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    
    res.status(200).json({
      success: true,
      user: {
        userName: user.userName,
        status: user.status,
        expiryDate: user.expiryDate,
        showInExpiringSoon: user.showInExpiringSoon
      },
      debug: {
        userExpiryDate: userExpiryDate.toISOString(),
        todayLocal: today.toISOString(),
        todayPKT: nowInPKT.toISOString(),
        isExpired: userExpiryDate <= today,
        shouldBeUnpaid: userExpiryDate <= today && user.status !== 'unpaid'
      }
    });
  } catch (error) {
    console.error('Debug error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============ CLEANUP ENDPOINT ============
// Clean up stale showInExpiringSoon flags for users not expiring within 2 days
app.post('/api/admin/cleanup-expiring-flags', ensureDbConnection, async (req, res) => {
  try {
    console.log('🧹 Cleaning up stale expiring-soon flags...');
    
    // Get current PKT date
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();
    
    const tomorrowInPKT = new Date(Date.UTC(todayY, todayM, todayD) + 24 * 60 * 60 * 1000);
    const tomorrowY = tomorrowInPKT.getUTCFullYear();
    const tomorrowM = tomorrowInPKT.getUTCMonth();
    const tomorrowD = tomorrowInPKT.getUTCDate();
    
    // Find all users with showInExpiringSoon flag
    const flaggedUsers = await usersCollection.find({
      showInExpiringSoon: true
    }).toArray();
    
    console.log(`📋 Found ${flaggedUsers.length} users with expiring-soon flag`);
    
    const parseExpiryYMD = (exp) => {
      if (!exp) return null;
      if (typeof exp === 'string') {
        const parts = exp.split(/[-\/]/);
        if (parts.length === 3) {
          const d = parseInt(parts[0], 10);
          const m = parseInt(parts[1], 10) - 1;
          const y = parseInt(parts[2], 10);
          if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
            return { y, m, d };
          }
        }
      }
      return null;
    };
    
    let cleanedCount = 0;
    
    for (const user of flaggedUsers) {
      const ymd = parseExpiryYMD(user.expiryDate);
      if (!ymd) {
        // Invalid expiry date - clear flag
        await usersCollection.updateOne(
          { _id: user._id },
          { $set: { showInExpiringSoon: false } }
        );
        cleanedCount++;
        console.log(`   ❌ Cleared flag for ${user.userName} (invalid expiry date)`);
        continue;
      }
      
      const isToday = (ymd.y === todayY && ymd.m === todayM && ymd.d === todayD);
      const isTomorrow = (ymd.y === tomorrowY && ymd.m === tomorrowM && ymd.d === tomorrowD);
      
      if (!isToday && !isTomorrow) {
        // Not expiring within 2 days - clear flag
        await usersCollection.updateOne(
          { _id: user._id },
          { $set: { showInExpiringSoon: false } }
        );
        cleanedCount++;
        console.log(`   ❌ Cleared flag for ${user.userName} (expires ${user.expiryDate}, not within 2 days)`);
      } else {
        console.log(`   ✅ Kept flag for ${user.userName} (expires ${user.expiryDate})`);
      }
    }
    
    console.log(`✅ Cleanup complete: ${cleanedCount} stale flags cleared`);
    
    res.status(200).json({
      success: true,
      message: 'Cleanup completed',
      totalFlagged: flaggedUsers.length,
      cleaned: cleanedCount,
      remaining: flaggedUsers.length - cleanedCount
    });
  } catch (error) {
    console.error('❌ Cleanup failed:', error);
    res.status(500).json({
      success: false,
      message: 'Cleanup failed',
      error: error.message
    });
  }
});

// ============ COLLECTIONS TRANSFER ROUTES ============

// POST transfer money from fee collector
app.post('/api/collections/transfer', ensureDbConnection, async (req, res) => {
  try {
    const { feeCollector, amount, message } = req.body;
    
    console.log('🔵 ===== TRANSFER API CALLED =====');
    console.log('🔵 Request body:', { feeCollector, amount, message });
    console.log('🔵 DB Status:', { isConnected, hasDb: !!db });
    
    // Ensure database is connected
    if (!db) {
      console.error('❌ Database not available');
      return res.status(503).json({
        success: false,
        message: 'Database connection not available'
      });
    }
    
    if (!feeCollector || !amount) {
      return res.status(400).json({
        success: false,
        message: 'Fee collector name and amount are required'
      });
    }
    
    const transferAmount = Number(amount);
    if (isNaN(transferAmount) || transferAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: 'Invalid amount'
      });
    }
    
    console.log('🔵 Processing transfer: Fee Collector =', feeCollector.trim(), '| Amount = Rs', transferAmount);
    
    // CRITICAL: Log database information
    console.log('🔵 Database Info:', {
      dbName: db.databaseName,
      namespace: db.namespace,
      client: !!db.client
    });
    
    // Get collections explicitly from db
    const incomesCol = db.collection('incomes');
    const vouchersCol = db.collection('vouchers');
    const transactionsCol = db.collection('transactions');
    console.log('🔵 Collections initialized:', { incomes: !!incomesCol, vouchers: !!vouchersCol, transactions: !!transactionsCol });
    console.log('🔵 Incomes collection namespace:', incomesCol.namespace);
    
    // Check if fee collector has enough income to transfer
    const feeCollectorIncome = await incomesCol.findOne({ name: feeCollector.trim() });
    let currentIncome = feeCollectorIncome?.cashIncome || 0;
    console.log(`💰 Fee collector current cashIncome: Rs${currentIncome}`);
    
    // Fallback: If incomes collection is empty/not synced, calculate from vouchers
    if (currentIncome === 0) {
      console.log('⚠️ Incomes collection not synced or cashIncome is 0, calculating from vouchers...');
      const vouchers = await vouchersCol.find({}).toArray();
      
      for (const voucher of vouchers) {
        if (voucher.months && Array.isArray(voucher.months)) {
          for (const month of voucher.months) {
            if (month.status === 'paid' || month.status === 'partial') {
              const paymentHistory = month.paymentHistory || [];
              
              if (paymentHistory.length > 0) {
                for (const payment of paymentHistory) {
                  if (payment.receivedBy && payment.receivedBy.toLowerCase() === feeCollector.trim().toLowerCase()) {
                    currentIncome += Number(payment.amount || 0);
                  }
                }
              } else if (month.receivedBy && month.receivedBy.toLowerCase() === feeCollector.trim().toLowerCase()) {
                currentIncome += Number(month.paidAmount || 0);
              }
            }
          }
        }
      }
      
      // Subtract transferred amounts
      const collectionsCollection = db.collection('collections');
      const transfers = await collectionsCollection.find({ 
        feeCollector: { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') } 
      }).toArray();
      
      for (const transfer of transfers) {
        currentIncome -= Number(transfer.amount || 0);
      }
      
      console.log(`💰 Calculated income from vouchers: Rs${currentIncome}`);
    }
    
    if (currentIncome < transferAmount) {
      return res.status(400).json({
        success: false,
        message: `Insufficient income. Available: Rs${currentIncome}, Requested: Rs${transferAmount}`
      });
    }
    
    // Create or get collections collection
    let collectionsCollection = db.collection('collections');
    
    // Check if collection exists, if not create it
    const collections = await db.listCollections().toArray();
    const collectionExists = collections.some(c => c.name === 'collections');
    if (!collectionExists) {
      await db.createCollection('collections');
      collectionsCollection = db.collection('collections');
    }
    
    // Create transfer record
    const transferRecord = {
      feeCollector: feeCollector.trim(),
      amount: transferAmount,
      message: message ? message.trim() : '',
      date: new Date(),
      createdAt: new Date()
    };
    
    const result = await collectionsCollection.insertOne(transferRecord);
    
    // 💰 UPDATE INCOME: Decrease fee collector's income and increase Admin's income
    // First, ensure the fee collector has an income record
    const feeCollectorTrimmed = feeCollector.trim();
    console.log('🔍 ===== INCOME UPDATE DEBUG =====');
    console.log('🔍 Searching for fee collector income record:', feeCollectorTrimmed);
    console.log('🔍 Search pattern:', `^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`);
    
    const feeCollectorIncomeRecord = await incomesCol.findOne({ 
      name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } 
    });
    console.log('🔍 Fee collector income record found:', feeCollectorIncomeRecord ? `Yes (ID: ${feeCollectorIncomeRecord._id}, Name: "${feeCollectorIncomeRecord.name}", Current cashIncome: Rs${feeCollectorIncomeRecord.cashIncome || 0})` : 'No');
    
    if (feeCollectorIncomeRecord) {
      // Update existing fee collector income - CUT from cashIncome ONLY
      console.log(`💰 Updating fee collector income: ${feeCollectorTrimmed} -Rs${transferAmount}`);
      console.log(`💰 Current cashIncome: Rs${feeCollectorIncomeRecord.cashIncome || 0}`);
      console.log(`💰 After deduction should be: Rs${(feeCollectorIncomeRecord.cashIncome || 0) - transferAmount}`);
      
      const updateResult = await incomesCol.updateOne(
        { name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
        { 
          $inc: { 
            cashIncome: -transferAmount  // Sirf cash se cut karo
          },
          $set: { lastUpdated: new Date() }
        }
      );
      console.log(`💰 Update result: matched=${updateResult.matchedCount}, modified=${updateResult.modifiedCount}, acknowledged=${updateResult.acknowledged}`);
      
      // Verify the update
      const verifyRecord = await incomesCol.findOne({ 
        name: { $regex: new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } 
      });
      console.log(`💰 After update verification: cashIncome is now Rs${verifyRecord?.cashIncome || 0}`);
      console.log('🔍 ===== END INCOME UPDATE DEBUG =====');
    } else {
      console.log(`⚠️ Warning: ${feeCollector} has no income record in incomes collection. Creating one with negative balance.`);
      // Create a new record with negative balance
      await incomesCol.insertOne({
        name: feeCollector.trim(),
        cashIncome: -transferAmount,  // Cash mein negative
        createdAt: new Date(),
        lastUpdated: new Date()
      });
      console.log(`💰 Created income record for ${feeCollector} with -Rs${transferAmount} in cash`);
    }
    
    // Update or create Admin income - ADD to cashIncome ONLY
    // Transfers are always cash, so only cashIncome increases
    const adminUpdateResult = await incomesCol.updateOne(
      { name: { $regex: new RegExp(`^Admin$`, 'i') } },
      { 
        $inc: { 
          cashIncome: transferAmount   // Sirf cash mein add karo
        },
        $set: { lastUpdated: new Date() },
        $setOnInsert: { 
          name: 'Admin', 
          createdAt: new Date()
          // bankIncome will be set separately if needed
        }
      },
      { upsert: true }
    );
    console.log(`💰 Income increased: Admin +Rs${transferAmount} in CASH ONLY (matched: ${adminUpdateResult.matchedCount}, modified: ${adminUpdateResult.modifiedCount}, upserted: ${adminUpdateResult.upsertedId || 'none'})`);
    
    // 📝 SAVE TRANSACTION: Store in transactions collection
    try {
      const transactionRecord = {
        type: 'transfer',
        from: feeCollector.trim(),
        to: 'Admin',
        amount: transferAmount,
        description: message ? message.trim() : `Transfer from ${feeCollector.trim()} to Admin`,
        date: new Date(),
        createdAt: new Date()
      };
      
      await transactionsCol.insertOne(transactionRecord);
      console.log(`📝 Transaction saved: ${feeCollector} -> Admin Rs${transferAmount}`);
    } catch (transactionError) {
      console.error('❌ Error saving transaction:', transactionError);
      // Don't fail the request if transaction save fails, just log the error
    }
    
    console.log(`💰 Transfer recorded: ${feeCollector} transferred Rs ${transferAmount}`);
    
    res.status(200).json({
      success: true,
      message: 'Money transferred successfully',
      data: {
        _id: result.insertedId,
        ...transferRecord
      }
    });
  } catch (error) {
    console.error('Error transferring money:', error);
    res.status(500).json({
      success: false,
      message: 'Error transferring money',
      error: error.message
    });
  }
});

// GET transferred amount for a specific fee collector
app.get('/api/collections/transferred', ensureDbConnection, async (req, res) => {
  try {
    const feeCollector = req.query.feeCollector;
    
    if (!feeCollector) {
      return res.status(400).json({
        success: false,
        message: 'Fee collector name is required'
      });
    }
    
    const collectionsCollection = db.collection('collections');
    
    // Calculate total transferred amount for this fee collector
    const transfers = await collectionsCollection.find({
      feeCollector: { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') }
    }).toArray();
    
    const totalTransferred = transfers.reduce((sum, transfer) => sum + Number(transfer.amount || 0), 0);
    
    res.status(200).json({
      success: true,
      data: {
        feeCollector: feeCollector.trim(),
        transferredAmount: totalTransferred,
        transferCount: transfers.length
      }
    });
  } catch (error) {
    console.error('Error fetching transferred amount:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching transferred amount',
      error: error.message
    });
  }
});

// GET all transferred amounts (for admin)
app.get('/api/collections/transferred/all', ensureDbConnection, async (req, res) => {
  try {
    const collectionsCollection = db.collection('collections');
    
    // Get all transfers
    const transfers = await collectionsCollection.find({}).toArray();
    
    // Group by fee collector and sum amounts
    const transferredMap = {};
    transfers.forEach((transfer) => {
      const fc = transfer.feeCollector?.trim() || '';
      if (fc) {
        if (!transferredMap[fc]) {
          transferredMap[fc] = 0;
        }
        transferredMap[fc] += Number(transfer.amount || 0);
      }
    });
    
    // Convert to array format
    const result = Object.entries(transferredMap).map(([feeCollector, transferredAmount]) => ({
      feeCollector,
      transferredAmount
    }));
    
    res.status(200).json({
      success: true,
      data: result
    });
  } catch (error) {
    console.error('Error fetching all transferred amounts:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching transferred amounts',
      error: error.message
    });
  }
});

// GET transfer history for a specific fee collector
app.get('/api/collections/transfers/history', ensureDbConnection, async (req, res) => {
  console.log('📥 Transfer history endpoint hit. Query:', req.query);
  try {
    const feeCollector = req.query.feeCollector;
    
    if (!feeCollector) {
      console.log('❌ Fee collector not provided');
      return res.status(400).json({
        success: false,
        message: 'Fee collector name is required'
      });
    }
    
    console.log('✅ Fetching transfer history for:', feeCollector);
    
    const collectionsCollection = db.collection('collections');
    
    // Get all transfers for this fee collector, sorted by date (newest first)
    const transfers = await collectionsCollection.find({
      feeCollector: { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') }
    }).sort({ date: -1 }).toArray();
    
    // Format transfers with date
    const formattedTransfers = transfers.map((transfer) => ({
      _id: transfer._id,
      amount: Number(transfer.amount || 0),
      message: transfer.message || '',
      date: transfer.date || transfer.createdAt,
      createdAt: transfer.createdAt
    }));
    
    res.status(200).json({
      success: true,
      data: formattedTransfers
    });
  } catch (error) {
    console.error('Error fetching transfer history:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching transfer history',
      error: error.message
    });
  }
});

// GET collection history for a specific fee collector
app.get('/api/collections/history/:feeCollector', ensureDbConnection, async (req, res) => {
  try {
    const feeCollectorName = req.params.feeCollector;
    
    if (!feeCollectorName) {
      return res.status(400).json({
        success: false,
        message: 'Fee collector name is required'
      });
    }
    
    console.log(`📜 Fetching collection history for: ${feeCollectorName}`);
    
    const vouchersCollection = db.collection('vouchers');
    
    // Get all vouchers
    const allVouchers = await vouchersCollection.find({}).toArray();
    console.log(`📜 Found ${allVouchers.length} total vouchers`);
    
    const collectionHistory = [];
    
    let sampleLogged = false;
    
    // Process vouchers to find payments received by this fee collector
    allVouchers.forEach(voucher => {
      // Try multiple fields to get user name
      const userName = voucher.userName || voucher.name || voucher.user || 'Unknown';
      
      // Handle multi-month vouchers
      if (voucher.months && Array.isArray(voucher.months)) {
        voucher.months.forEach(month => {
          if (month.status === 'paid' && month.receivedBy) {
            const receivedByLower = month.receivedBy.toLowerCase().trim();
            const feeCollectorLower = feeCollectorName.toLowerCase().trim();
            
            if (receivedByLower === feeCollectorLower) {
              const paidAmount = Number(month.paidAmount || 0);
              if (paidAmount > 0) {
                collectionHistory.push({
                  userName: userName,
                  amount: paidAmount,
                  date: month.paymentDate || month.paidDate || new Date(),
                  paymentMethod: month.paymentMethod || 'Cash',
                  month: month.month
                });
              }
            }
          }
        });
      }
      // Handle old single-month vouchers
      else if (voucher.status === 'paid') {
        // Check receivedBy field
        const receivedBy = voucher.receivedBy || '';
        const receivedByLower = receivedBy.toLowerCase().trim();
        const feeCollectorLower = feeCollectorName.toLowerCase().trim();
        
        // Match if receivedBy matches fee collector name
        if (receivedByLower === feeCollectorLower) {
          const paidAmount = Number(voucher.paidAmount || 0);
          if (paidAmount > 0) {
            collectionHistory.push({
              userName: userName,
              amount: paidAmount,
              date: voucher.paymentDate || voucher.paidDate || new Date(),
              paymentMethod: voucher.paymentMethod || 'Cash'
            });
          }
        }
        // Fallback: Check if user is assigned to this fee collector
        else if (!receivedBy || receivedBy === 'Myself' || receivedBy === 'Admin') {
          const userFeeCollector = voucher.feeCollector || '';
          const userFeeCollectorLower = userFeeCollector.toLowerCase().trim();
          
          if (userFeeCollectorLower === feeCollectorLower) {
            const paidAmount = Number(voucher.paidAmount || 0);
            if (paidAmount > 0) {
              collectionHistory.push({
                userName: userName,
                amount: paidAmount,
                date: voucher.paymentDate || voucher.paidDate || new Date(),
                paymentMethod: voucher.paymentMethod || 'Cash'
              });
            }
          }
        }
      }
      
      // Check payment history in multi-month vouchers
      if (voucher.paymentHistory && Array.isArray(voucher.paymentHistory)) {
        voucher.paymentHistory.forEach(payment => {
          const receivedByLower = (payment.receivedBy || '').toLowerCase().trim();
          const feeCollectorLower = feeCollectorName.toLowerCase().trim();
          
          if (receivedByLower === feeCollectorLower) {
            const paidAmount = Number(payment.amount || 0);
            if (paidAmount > 0) {
              collectionHistory.push({
                userName: userName,
                amount: paidAmount,
                date: payment.date || new Date(),
                paymentMethod: payment.paymentMethod || 'Cash',
                month: payment.month
              });
            }
          }
        });
      }
    });
    
    // Sort by date (most recent first)
    collectionHistory.sort((a, b) => {
      const dateA = new Date(a.date);
      const dateB = new Date(b.date);
      return dateB.getTime() - dateA.getTime();
    });
    
    console.log(`✅ Found ${collectionHistory.length} collection records for ${feeCollectorName}`);
    
    // Log sample data for debugging
    if (collectionHistory.length > 0) {
      console.log('📝 Sample collection record:', {
        userName: collectionHistory[0].userName,
        amount: collectionHistory[0].amount,
        paymentMethod: collectionHistory[0].paymentMethod
      });
    }
    
    res.status(200).json({
      success: true,
      data: collectionHistory
    });
  } catch (error) {
    console.error('❌ Error fetching collection history:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching collection history',
      error: error.message
    });
  }
});

// ==================== INCOMES API ====================

// GET all incomes
app.get('/api/incomes', ensureDbConnection, async (req, res) => {
  try {
    const incomes = await incomesCollection.find({}).sort({ name: 1 }).toArray();
    
    res.status(200).json({
      success: true,
      data: incomes
    });
  } catch (error) {
    console.error('Error fetching incomes:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching incomes',
      error: error.message
    });
  }
});

// GET income for a specific person
app.get('/api/incomes/:name', ensureDbConnection, async (req, res) => {
  try {
    const name = req.params.name;
    
    // Use case-insensitive search to match transfer endpoint logic
    const income = await incomesCollection.findOne({ 
      name: { $regex: new RegExp(`^${name.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } 
    });
    
    if (!income) {
      return res.status(404).json({
        success: false,
        message: 'Income record not found',
        data: { name, cashIncome: 0, bankIncome: 0 }
      });
    }
    
    res.status(200).json({
      success: true,
      data: income
    });
  } catch (error) {
    console.error('Error fetching income:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching income',
      error: error.message
    });
  }
});

// POST - Add income when payment is received
app.post('/api/incomes', ensureDbConnection, async (req, res) => {
  try {
    const { receivedBy, amount, paymentMethod } = req.body;
    
    if (!receivedBy || !amount || amount <= 0) {
      return res.status(400).json({
        success: false,
        message: 'receivedBy and valid amount are required'
      });
    }

    console.log(`💰 Adding income: ${receivedBy} - Rs ${amount} (${paymentMethod || 'Cash'})`);

    // CRITICAL: Income storage logic based on who receives payment:
    // - Employee: ALWAYS cashIncome (regardless of payment method Cash/Bank)
    // - Admin: Cash → cashIncome, Bank → bankIncome
    
    const receivedByTrimmed = receivedBy.trim();
    const isAdmin = receivedByTrimmed.toLowerCase() === 'admin' || receivedByTrimmed.toLowerCase() === 'myself';
    
    // Determine which field to update
    let updateField = 'cashIncome';
    if (isAdmin) {
      // Admin: respect payment method
      const isCash = !paymentMethod || paymentMethod === 'Cash' || paymentMethod === 'cash';
      updateField = isCash ? 'cashIncome' : 'bankIncome';
    } else {
      // Employee: always cashIncome
      updateField = 'cashIncome';
    }
    
    // Check if income record exists for this person
    const existingIncome = await incomesCollection.findOne({
      name: { $regex: new RegExp(`^${receivedByTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }
    });

    if (existingIncome) {
      // Update existing income
      await incomesCollection.updateOne(
        { name: { $regex: new RegExp(`^${receivedByTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } },
        {
          $inc: { [updateField]: amount },
          $set: { lastUpdated: new Date() }
        }
      );
      console.log(`💰 Updated income for ${receivedBy}: ${updateField === 'cashIncome' ? 'Cash' : 'Bank'} +Rs ${amount}`);
    } else {
      // Create new income record
      const newIncome = {
        name: receivedByTrimmed,
        cashIncome: updateField === 'cashIncome' ? amount : 0,
        bankIncome: updateField === 'bankIncome' ? amount : 0,
        createdAt: new Date(),
        lastUpdated: new Date()
      };
      await incomesCollection.insertOne(newIncome);
      console.log(`💰 Created new income record for ${receivedBy}: ${updateField === 'cashIncome' ? 'Cash' : 'Bank'} Rs ${amount}`);
    }

    // Get updated income record
    const updatedIncome = await incomesCollection.findOne({
      name: { $regex: new RegExp(`^${receivedByTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') }
    });

    res.status(200).json({
      success: true,
      message: 'Income updated successfully',
      data: updatedIncome
    });
  } catch (error) {
    console.error('Error adding income:', error);
    res.status(500).json({
      success: false,
      message: 'Error adding income',
      error: error.message
    });
  }
});

// POST - Update income manually (for admin to change technician income)
app.post('/api/incomes/update', ensureDbConnection, async (req, res) => {
  try {
    const { name, cashIncome } = req.body;
    
    if (!name) {
      return res.status(400).json({
        success: false,
        message: 'Name is required'
      });
    }
    
    const incomeValue = Number(cashIncome || 0);
    console.log(`💰 Updating income for ${name} to Rs ${incomeValue}`);
    
    // Find existing income record
    const existingIncome = await incomesCollection.findOne({ name: name });
    
    if (existingIncome) {
      // Update existing record
      await incomesCollection.updateOne(
        { name: name },
        { 
          $set: { 
            cashIncome: incomeValue,
            updatedAt: new Date()
          } 
        }
      );
      console.log(`✅ Updated income for ${name}`);
    } else {
      // Create new record
      await incomesCollection.insertOne({
        name: name,
        cashIncome: incomeValue,
        bankIncome: 0,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      console.log(`✅ Created new income record for ${name}`);
    }
    
    res.status(200).json({
      success: true,
      message: `Income updated to Rs ${incomeValue} for ${name}`,
      data: {
        name: name,
        cashIncome: incomeValue
      }
    });
  } catch (error) {
    console.error('❌ Error updating income:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating income',
      error: error.message
    });
  }
});

// POST - Sync incomes from existing vouchers (one-time initialization)
app.post('/api/incomes/sync', ensureDbConnection, async (req, res) => {
  try {
    console.log('💰 Starting income sync from existing vouchers...');
    
    // Clear existing incomes (fresh start)
    await incomesCollection.deleteMany({});
    console.log('💰 Cleared existing incomes');
    
    // Get all vouchers
    const vouchers = await vouchersCollection.find({}).toArray();
    console.log(`💰 Found ${vouchers.length} vouchers to process`);
    
    let totalProcessed = 0;
    const incomeMap = {}; // Temporary map to accumulate incomes
    
    // Process each voucher
    for (const voucher of vouchers) {
      if (voucher.months && Array.isArray(voucher.months)) {
        for (const month of voucher.months) {
          if (month.status === 'paid' || month.status === 'partial') {
            const paymentHistory = month.paymentHistory || [];
            
            if (paymentHistory.length > 0) {
              // New structure: Process each payment in history
              for (const payment of paymentHistory) {
                const receiver = payment.receivedBy || 'Admin';
                const amount = parseFloat(payment.amount) || 0;
                
                if (amount > 0) {
                  if (!incomeMap[receiver]) {
                    incomeMap[receiver] = 0;
                  }
                  incomeMap[receiver] += amount;
                  totalProcessed++;
                }
              }
            } else if (month.receivedBy) {
              // Old structure: Single receivedBy field
              const receiver = month.receivedBy;
              const amount = parseFloat(month.paidAmount) || 0;
              
              if (amount > 0) {
                if (!incomeMap[receiver]) {
                  incomeMap[receiver] = 0;
                }
                incomeMap[receiver] += amount;
                totalProcessed++;
              }
            }
          }
        }
      }
    }
    
    // Insert accumulated incomes into collection (ONLY cashIncome)
    const incomeRecords = Object.entries(incomeMap).map(([name, cashIncome]) => ({
      name,
      cashIncome,
      createdAt: new Date(),
      lastUpdated: new Date()
    }));
    
    if (incomeRecords.length > 0) {
      await incomesCollection.insertMany(incomeRecords);
      console.log(`💰 Inserted ${incomeRecords.length} income records`);
    }
    
    // Subtract transferred amounts from fee collectors (ONLY cashIncome)
    const collectionsCollection = db.collection('collections');
    const transfers = await collectionsCollection.find({}).toArray();
    
    for (const transfer of transfers) {
      const feeCollector = transfer.feeCollector?.trim();
      const amount = Number(transfer.amount || 0);
      
      if (feeCollector && amount > 0) {
        // Decrease fee collector's cashIncome
        await incomesCollection.updateOne(
          { name: feeCollector },
          { 
            $inc: { cashIncome: -amount },
            $set: { lastUpdated: new Date() }
          }
        );
        
        // Increase Admin's cashIncome
        await incomesCollection.updateOne(
          { name: 'Admin' },
          { 
            $inc: { cashIncome: amount },
            $set: { lastUpdated: new Date() },
            $setOnInsert: { name: 'Admin', createdAt: new Date() }
          },
          { upsert: true }
        );
      }
    }
    
    console.log(`💰 Processed ${transfers.length} transfers`);
    
    // Get final incomes
    const finalIncomes = await incomesCollection.find({}).sort({ cashIncome: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      message: 'Income sync completed successfully',
      data: {
        totalPaymentsProcessed: totalProcessed,
        totalTransfersProcessed: transfers.length,
        incomeRecords: finalIncomes
      }
    });
  } catch (error) {
    console.error('Error syncing incomes:', error);
    res.status(500).json({
      success: false,
      message: 'Error syncing incomes',
      error: error.message
    });
  }
});

// ==================== COMPLAINTS API ====================

// POST - Create a new complaint
app.post('/api/complaints', ensureDbConnection, async (req, res) => {
  try {
    const { userId, userName, message, assignTo, simNo, whatsappNo, reportedBy } = req.body;
    
    if (!userId || !userName || !message || !message.trim()) {
      return res.status(400).json({
        success: false,
        message: 'User ID, user name, and message are required'
      });
    }
    
    // Create or get complaints collection
    let complaintsCollection = db.collection('complaints');
    
    // Check if collection exists, if not create it
    const collections = await db.listCollections().toArray();
    const collectionExists = collections.some(c => c.name === 'complaints');
    if (!collectionExists) {
      await db.createCollection('complaints');
      complaintsCollection = db.collection('complaints');
    }
    
    // Create complaint record
    const complaintRecord = {
      userId: userId,
      userName: userName.trim(),
      message: message.trim(),
      assignTo: assignTo ? assignTo.trim() : null,
      simNo: simNo || null,
      whatsappNo: whatsappNo || null,
      reportedBy: reportedBy ? reportedBy.trim() : null, // Who reported: admin or fee collector name
      status: 'pending', // pending, resolved
      createdAt: new Date(),
      updatedAt: new Date()
    };
    
    const result = await complaintsCollection.insertOne(complaintRecord);
    
    console.log(`📝 Complaint created: ${userName} - ${message.substring(0, 50)}...`);
    
    // Create notification for admin if complaint is not submitted by admin
    if (reportedBy && reportedBy.toLowerCase() !== 'admin') {
      try {
        await notificationsCollection.insertOne({
          type: 'new_complaint',
          title: 'New Complaint Received',
          message: `${reportedBy} submitted a complaint from ${userName}`,
          complaintId: result.insertedId.toString(),
          userName: userName.trim(),
          complaintMessage: message.trim(),
          submittedBy: reportedBy.trim(),
          isRead: false,
          createdAt: new Date()
        });
        console.log('✅ Notification created for admin');
      } catch (notifError) {
        console.error('Error creating notification:', notifError);
      }
    }
    
    res.status(200).json({
      success: true,
      message: 'Complaint submitted successfully',
      data: {
        _id: result.insertedId,
        ...complaintRecord
      }
    });
  } catch (error) {
    console.error('Error creating complaint:', error);
    res.status(500).json({
      success: false,
      message: 'Error creating complaint',
      error: error.message
    });
  }
});

// GET - Get all complaints (with optional filters)
app.get('/api/complaints', ensureDbConnection, async (req, res) => {
  try {
    const technician = req.query.technician; // Filter by assigned technician
    const status = req.query.status; // Filter by status (pending, resolved)
    const reportedBy = req.query.reportedBy; // Filter by who reported (fee collector name)
    const role = req.query.role; // User role: admin, fee collector, technician
    const search = req.query.search; // Search by name or phone number
    const page = parseInt(req.query.page) || 1; // Page number (default: 1)
    const limit = parseInt(req.query.limit) || 20; // Items per page (default: 20)
    
    const complaintsCollection = db.collection('complaints');
    
    // Build query
    const query = {};
    if (technician) {
      query.assignTo = { $regex: new RegExp(`^${technician.trim()}$`, 'i') };
    }
    if (status) {
      query.status = status.toLowerCase();
    }
    // For fee collector: show only complaints they reported
    if (role === 'fee collector' && reportedBy) {
      query.reportedBy = { $regex: new RegExp(`^${reportedBy.trim()}$`, 'i') };
    }
    
    // Apply search filter to query if provided
    if (search && search.trim() !== '') {
      const searchTerm = search.trim();
      query.$or = [
        { userName: { $regex: searchTerm, $options: 'i' } },
        { message: { $regex: searchTerm, $options: 'i' } },
        { simNo: { $regex: searchTerm, $options: 'i' } },
        { whatsappNo: { $regex: searchTerm, $options: 'i' } }
      ];
    }
    
    // Calculate skip value for pagination
    const skip = (page - 1) * limit;
    
    // Get total count for pagination
    const totalCount = await complaintsCollection.countDocuments(query);
    
    // Get complaints with pagination, sorted by date (newest first)
    const complaints = await complaintsCollection
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .toArray();
    
    // Format complaints
    const formattedComplaints = complaints.map((complaint) => ({
      _id: complaint._id,
      userId: complaint.userId,
      userName: complaint.userName || '',
      message: complaint.message || '',
      assignTo: complaint.assignTo || null,
      simNo: complaint.simNo || null,
      whatsappNo: complaint.whatsappNo || null,
      reportedBy: complaint.reportedBy || null,
      status: complaint.status || 'pending',
      createdAt: complaint.createdAt,
      updatedAt: complaint.updatedAt || complaint.createdAt
    }));
    
    res.status(200).json({
      success: true,
      data: formattedComplaints,
      count: formattedComplaints.length,
      total: totalCount,
      page: page,
      totalPages: Math.ceil(totalCount / limit),
      hasMore: skip + formattedComplaints.length < totalCount
    });
  } catch (error) {
    console.error('❌ Error fetching complaints:', error);
    console.error('❌ Error stack:', error.stack);
    
    // Ensure we always return valid JSON
    try {
      res.status(500).json({
        success: false,
        message: 'Error fetching complaints',
        error: error.message || 'Unknown error'
      });
    } catch (jsonError) {
      // If JSON response fails, send plain text (shouldn't happen, but safety)
      console.error('❌ Failed to send JSON error response:', jsonError);
      res.status(500).send(`Error: ${error.message || 'Unknown error'}`);
    }
  }
});

// GET - Get complaint statistics for a technician
app.get('/api/complaints/stats', ensureDbConnection, async (req, res) => {
  try {
    const technician = req.query.technician;
    
    if (!technician) {
      return res.status(400).json({
        success: false,
        message: 'Technician name is required'
      });
    }
    
    const complaintsCollection = db.collection('complaints');
    
    // Get all complaints for this technician
    const complaints = await complaintsCollection.find({
      assignTo: { $regex: new RegExp(`^${technician.trim()}$`, 'i') }
    }).toArray();
    
    // Calculate stats
    const pending = complaints.filter(c => (c.status || 'pending').toLowerCase() === 'pending').length;
    const resolved = complaints.filter(c => (c.status || 'pending').toLowerCase() === 'resolved').length;
    const total = complaints.length;
    
    res.status(200).json({
      success: true,
      data: {
        total,
        pending,
        resolved
      }
    });
  } catch (error) {
    console.error('Error fetching complaint stats:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching complaint statistics',
      error: error.message
    });
  }
});

// PUT - Update complaint status
app.put('/api/complaints/:id/status', ensureDbConnection, async (req, res) => {
  try {
    const complaintId = req.params.id;
    const { status, resolvedBy } = req.body;
    
    if (!complaintId) {
      return res.status(400).json({
        success: false,
        message: 'Complaint ID is required'
      });
    }
    
    if (!status || !['pending', 'resolved'].includes(status.toLowerCase())) {
      return res.status(400).json({
        success: false,
        message: 'Status must be either "pending" or "resolved"'
      });
    }
    
    const complaintsCollection = db.collection('complaints');
    
    // Get complaint details first for notification
    const complaint = await complaintsCollection.findOne({ _id: new ObjectId(complaintId) });
    
    if (!complaint) {
      return res.status(404).json({
        success: false,
        message: 'Complaint not found'
      });
    }
    
    // Update complaint status
    const result = await complaintsCollection.updateOne(
      { _id: new ObjectId(complaintId) },
      {
        $set: {
          status: status.toLowerCase(),
          updatedAt: new Date()
        }
      }
    );
    
    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Complaint not found'
      });
    }
    
    // CRITICAL: Create notification for admin when complaint is resolved
    // Check if status was changed from pending to resolved AND resolver is not admin
    const wasResolved = complaint.status && complaint.status.toLowerCase() === 'resolved';
    const isNowResolved = status.toLowerCase() === 'resolved';
    const resolver = resolvedBy || complaint.assignTo || complaint.reportedBy || 'Unknown';
    const isResolverAdmin = resolver.toLowerCase().includes('admin');
    
    console.log('📊 Notification check:', {
      complaintId,
      wasResolved,
      isNowResolved,
      resolver,
      isResolverAdmin,
      shouldCreateNotification: !wasResolved && isNowResolved && !isResolverAdmin
    });
    
    // Create notification if: complaint is being marked as resolved (not already resolved) AND resolver is not admin
    if (!wasResolved && isNowResolved && !isResolverAdmin) {
      try {
        const notificationData = {
          type: 'complaint_resolved',
          title: 'Complaint Resolved',
          message: `${resolver} resolved complaint from ${complaint.userName}`,
          complaintId: complaintId,
          userName: complaint.userName,
          userId: complaint.userId,
          whatsappNo: complaint.whatsappNo || null,
          simNo: complaint.simNo || null,
          complaintMessage: complaint.message,
          resolvedBy: resolver,
          isRead: false,
          createdAt: new Date()
        };
        
        await notificationsCollection.insertOne(notificationData);
        console.log(`✅ Notification created for admin (complaint resolved by ${resolver})`);
        console.log('📧 Notification data:', notificationData);
      } catch (notifError) {
        console.error('❌ Error creating notification:', notifError);
      }
    } else {
      console.log('⏭️ Skipping notification creation');
    }
    
    console.log(`✅ Complaint ${complaintId} status updated to ${status}`);
    
    res.status(200).json({
      success: true,
      message: 'Complaint status updated successfully',
      data: {
        _id: complaintId,
        status: status.toLowerCase()
      }
    });
  } catch (error) {
    console.error('Error updating complaint status:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating complaint status',
      error: error.message
    });
  }
});

// DELETE - Delete a complaint
app.delete('/api/complaints/:id', ensureDbConnection, async (req, res) => {
  try {
    const complaintId = req.params.id;
    
    if (!complaintId) {
      return res.status(400).json({
        success: false,
        message: 'Complaint ID is required'
      });
    }
    
    // Validate ObjectId format
    if (!ObjectId.isValid(complaintId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid complaint ID format'
      });
    }
    
    const complaintsCollection = db.collection('complaints');
    
    // Delete complaint
    const result = await complaintsCollection.deleteOne(
      { _id: new ObjectId(complaintId) }
    );
    
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Complaint not found'
      });
    }
    
    console.log(`🗑️ Complaint ${complaintId} deleted`);
    
    res.status(200).json({
      success: true,
      message: 'Complaint deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting complaint:', error);
    res.status(500).json({
      success: false,
      message: 'Error deleting complaint',
      error: error.message
    });
  }
});

// Export for Vercel serverless
// POST endpoint to recalculate user status from vouchers
app.post('/api/users/:id/recalculate-status', async (req, res) => {
  try {
    const userId = req.params.id;
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    
    console.log(`🔄 Recalculating status for user: ${userId}`);
    
    // Fetch user
    const user = await usersCollection.findOne({ _id: new ObjectId(userId) });
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    // Fetch vouchers directly from database
    const userVouchers = await vouchersCollection.find({
      $or: [
        { userId: userId },
        { userId: user._id.toString() }
      ]
    }).toArray();
    
    if (!userVouchers || userVouchers.length === 0) {
      // No vouchers - set status to unpaid
      await usersCollection.updateOne(
        { _id: new ObjectId(userId) },
        { $set: { status: 'unpaid', paidAmount: 0, remainingAmount: user.amount || 0 } }
      );
      return res.status(200).json({ success: true, message: 'Status updated to unpaid (no vouchers)', status: 'unpaid' });
    }
    
    let totalPaid = 0;
    let totalRemaining = 0;
    let hasUnpaidMonth = false;
    
    for (const v of userVouchers) {
      if (Array.isArray(v.months)) {
        for (const m of v.months) {
          const isReversed = !!(m.refundDate || m.refundedAmount);
          if (isReversed) continue;
          
          if (m.status === 'unpaid') {
            hasUnpaidMonth = true;
          }
          
          const monthPaid = Number(m.paidAmount || 0);
          const pkg = Number(m.packageFee || 0);
          const disc = Number(m.discount || 0);
          const monthRemaining = (m.remainingAmount !== undefined && m.remainingAmount !== null)
            ? Number(m.remainingAmount)
            : Math.max(0, pkg - disc - monthPaid);
          totalPaid += monthPaid;
          totalRemaining += monthRemaining;
        }
      } else {
        const isReversed = !!(v.refundDate || v.refundedAmount);
        if (isReversed) continue;
        
        if (v.status === 'unpaid') {
          hasUnpaidMonth = true;
        }
        
        const monthPaid = Number(v.paidAmount || 0);
        const pkg = Number(v.packageFee || v.amount || 0);
        const disc = Number(v.discount || 0);
        const monthRemaining = (v.remainingAmount !== undefined && v.remainingAmount !== null)
          ? Number(v.remainingAmount)
          : Math.max(0, pkg - disc - monthPaid);
        totalPaid += monthPaid;
        totalRemaining += monthRemaining;
      }
    }
    
    // Calculate status based on new logic
    let calculatedStatus = 'unpaid';
    if (totalRemaining <= 0) {
      calculatedStatus = 'paid';
    } else if (hasUnpaidMonth) {
      // CRITICAL: If ANY month is unpaid, user should be in Unpaid section
      calculatedStatus = 'unpaid';
    } else if (totalPaid > 0 && totalRemaining > 0) {
      // Only set to 'partial' if user has made payment AND has remaining BUT no unpaid months
      calculatedStatus = 'partial';
    }
    
    console.log(`📊 Recalculated status: ${calculatedStatus} (totalPaid: ${totalPaid}, totalRemaining: ${totalRemaining}, hasUnpaidMonth: ${hasUnpaidMonth})`);
    
    // Update user status
    await usersCollection.updateOne(
      { _id: new ObjectId(userId) },
      { $set: { status: calculatedStatus, paidAmount: totalPaid, remainingAmount: totalRemaining } }
    );
    
    res.status(200).json({
      success: true,
      message: 'Status recalculated successfully',
      status: calculatedStatus,
      totalPaid,
      totalRemaining,
      hasUnpaidMonth
    });
  } catch (error) {
    console.error('Error recalculating user status:', error);
    res.status(500).json({ success: false, message: 'Error recalculating status', error: error.message });
  }
});

module.exports = app;
