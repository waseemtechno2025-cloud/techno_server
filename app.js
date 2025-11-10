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
      expenses: !!expensesCollection
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
    
    // Check if expiry date is TOMORROW - if yes, set showInExpiringSoon flag immediately
    const tomorrowDate = new Date(Date.UTC(todayY, todayM, todayD + 1));
    const tomorrowY = tomorrowDate.getUTCFullYear();
    const tomorrowM = tomorrowDate.getUTCMonth();
    const tomorrowD = tomorrowDate.getUTCDate();
    
    const isExpiringTomorrow = expiryYMD && 
      expiryYMD.y === tomorrowY && 
      expiryYMD.m === tomorrowM && 
      expiryYMD.d === tomorrowD;
    
    if (isExpiringTomorrow) {
      console.log('🔔 User expires TOMORROW - Setting showInExpiringSoon flag immediately');
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
      rechargeDate: rechargeDate || null,
      expiryDate: expiryDate || null,
      status: paymentStatus, // Payment status: paid, unpaid, partial, pending
      serviceStatus: 'active', // Service status: always active for new users
      paidAmount: paidAmount,
      remainingAmount: remainingAmount,
      showInExpiringSoon: isExpiringTomorrow, // Set immediately if expires tomorrow
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
    if (rechargeDate !== undefined) updateFields.rechargeDate = rechargeDate || null;
    if (expiryDate !== undefined) updateFields.expiryDate = expiryDate || null;
    if (networkType !== undefined) updateFields.networkType = networkType || 'local';
    if (status !== undefined) updateFields.status = status;
    if (serviceStatus !== undefined) updateFields.serviceStatus = serviceStatus;
    if (paidAmount !== undefined) updateFields.paidAmount = paidAmount || 0;
    if (remainingAmount !== undefined) updateFields.remainingAmount = remainingAmount || 0;

    if (Object.keys(updateFields).length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }
    
    // Check if expiry date is being updated and if it's TOMORROW
    if (expiryDate !== undefined) {
      const nowUTC = new Date();
      const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
      const todayY = nowInPKT.getUTCFullYear();
      const todayM = nowInPKT.getUTCMonth();
      const todayD = nowInPKT.getUTCDate();
      
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
      const isExpiringTomorrow = expiryYMD && 
        expiryYMD.y === tomorrowY && 
        expiryYMD.m === tomorrowM && 
        expiryYMD.d === tomorrowD;
      
      if (isExpiringTomorrow) {
        console.log('🔔 User expires TOMORROW - Setting showInExpiringSoon flag');
        updateFields.showInExpiringSoon = true;
      } else {
        // If expiry date is not tomorrow, remove flag
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
    
    // First delete all vouchers for this user
    const vouchersResult = await vouchersCollection.deleteMany({ userId: req.params.id });
    console.log(`Deleted ${vouchersResult.deletedCount} vouchers for user ${req.params.id}`);
    
    // Then delete the user
    const result = await usersCollection.deleteOne({ _id: userId });
    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }
    
    res.status(200).json({
      success: true,
      message: 'User and associated vouchers deleted successfully',
      deletedVouchersCount: vouchersResult.deletedCount
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
    const { name, number, role, salary, isActive } = req.body;

    if (!name || !number || !role || salary === undefined) {
      return res.status(400).json({
        success: false,
        message: 'Name, number, role, and salary are required'
      });
    }

    const employee = {
      name: name.trim(),
      number: number.trim(),
      role: role.trim(),
      salary: parseFloat(salary),
      isActive: isActive !== undefined ? isActive : true,
      createdAt: new Date()
    };

    const result = await employeesCollection.insertOne(employee);
    
    res.status(201).json({
      success: true,
      message: 'Employee added successfully',
      data: {
        _id: result.insertedId,
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
    const { name, number, role, salary, isActive } = req.body;

    if (!name || !number || !role || salary === undefined) {
      return res.status(400).json({
        success: false,
        message: 'Name, number, role, and salary are required'
      });
    }

    const result = await employeesCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { 
        $set: { 
          name: name.trim(), 
          number: number.trim(), 
          role: role.trim(), 
          salary: parseFloat(salary),
          isActive: isActive !== undefined ? isActive : true
        } 
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Employee not found'
      });
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
    
    // Total users
    const totalUsers = await usersCollection.countDocuments();
    
    // Paid users (includes both 'paid' and 'partial' status, exclude inactive)
    const paidUsers = await usersCollection.countDocuments({
      status: { $in: ['paid', 'partial'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    });
    
    // Unpaid users (exclude inactive)
    const unpaidUsers = await usersCollection.countDocuments({
      status: 'unpaid',
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    });
    
    // Expiring soon (TOMORROW) - include paid/partial/unpaid/pending users based on expiry date
    // NOTE: Include unpaid and pending so "Pay Later" and checkbox users also count when expiring tomorrow
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const endOfTomorrow = new Date(tomorrow);
    endOfTomorrow.setHours(23, 59, 59, 999);
    
    const expiringSoon = await usersCollection.countDocuments({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
      expiryDate: { 
        $gte: tomorrow.toISOString(), 
        $lte: endOfTomorrow.toISOString() 
      },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    });
    
    // Deactivated users
    const deactivatedUsers = await usersCollection.countDocuments({
      status: 'inactive'
    });
    
    // Total income (sum of all paidAmount from users)
    const allUsers = await usersCollection.find({}).toArray();
    const totalIncome = allUsers.reduce((sum, user) => sum + Number(user.paidAmount || 0), 0);
    
    // Total expense
    const expenseResult = await transactionsCollection.aggregate([
      { $match: { type: 'expense' } },
      { $group: { _id: null, total: { $sum: '$amount' } } }
    ]).toArray();
    const totalExpense = expenseResult.length > 0 ? expenseResult[0].total : 0;
    
    // Outstanding/Balance - Calculate from vouchers to match unpaid-users.tsx display amounts
    const vouchersCollection = db.collection('vouchers');
    const allVouchers = await vouchersCollection.find({}).toArray();
    
    // Get unpaid and partial users (same as unpaid-users.tsx)
    const unpaidUsersList = await usersCollection.find({
      status: 'unpaid',
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    }).toArray();
    
    const partialUsers = await usersCollection.find({
      status: 'partial',
      remainingAmount: { $gt: 0 },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    }).toArray();
    
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
    
    res.status(200).json({
      success: true,
      data: {
        totalUsers,
        paidUsers,
        totalIncome,
        totalExpense,
        unpaidUsers,
        outstanding,
        balance: outstanding, // Same as outstanding - sum of remainingAmount from partial users
        balanceCustomers, // Number of customers with remaining balance
        expiringSoon,
        deactivatedUsers
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
    const paymentDate = req.query.paymentDate; // YYYY-MM-DD format
    
    let userIds = [];
    
    // If payment date filter is provided, focus on vouchers that recorded paid/partial activity that day
    if (paymentDate) {
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

        const vouchers = await vouchersCollection.find({ 'months.status': { $in: ['paid', 'partial'] } }).toArray();

        const filteredVouchers = vouchers.filter((voucher) => {
          const months = Array.isArray(voucher.months) ? voucher.months : [];
          const paidOrPartialMonths = months.filter((month) => ['paid', 'partial'].includes(month.status));

          const monthMatches = paidOrPartialMonths.some((month) => {
            if (matchesPaymentDate(month.createdAt) || matchesPaymentDate(month.date)) {
              return true;
            }
            if (Array.isArray(month.paymentHistory)) {
              return month.paymentHistory.some((entry) => matchesPaymentDate(entry?.date));
            }
            return false;
          });

          if (monthMatches) {
            return true;
          }

          const topLevelMatch = (matchesPaymentDate(voucher.createdAt) || matchesPaymentDate(voucher.updatedAt));
          return topLevelMatch && paidOrPartialMonths.length > 0;
        });

        console.log(`📊 Query result: Found ${filteredVouchers.length} vouchers matching ${paymentDate}`);
        filteredVouchers.forEach(v => {
          console.log(`  - User: ${v.userName}, voucherCreated: ${v.createdAt}, months: ${(v.months || []).length}`);
        });

        userIds = filteredVouchers.map(v => v.userId);
        console.log(`Found ${userIds.length} user(s) with paid/partial activity on ${paymentDate}`);
      } else {
        console.log(`⚠️ Invalid paymentDate received: ${paymentDate}`);
      }
    }
    
    // Base query - include both fully paid and partially paid users
    let query = {
      status: { $in: ['paid', 'partial'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    
    // Add user ID filter if recharge date was provided
    if (paymentDate && userIds.length > 0) {
      const objectIds = userIds.map(id => new ObjectId(id));
      query._id = { $in: objectIds };
      console.log(`🔍 Filtering users with IDs:`, objectIds.map(id => id.toString()));
    } else if (paymentDate && userIds.length === 0) {
      // No users found for this recharge date
      return res.status(200).json({
        success: true,
        data: [],
        totalCount: 0,
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

// GET unpaid users (with date filter and pagination)
app.get('/api/users/unpaid', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const expiryDate = req.query.expiryDate; // YYYY-MM-DD format
    
    // Base query - Only show unpaid status (exclude pending) and active users
    let query = {
      status: 'unpaid',
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        }
      ]
    };
    
    // If expiry date filter is provided, check both vouchers and users collections
    if (expiryDate) {
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
    
    const totalCount = await usersCollection.countDocuments(query);
    const users = await usersCollection
      .find(query)
      .sort({ userName: 1 })
      .skip((page - 1) * limit)
      .limit(limit)
      .toArray();
    
    console.log(`Unpaid users: ${users.length} found, ${totalCount} total`);
    
    res.status(200).json({
      success: true,
      data: users,
      totalCount,
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
    
    // Fetch user details with pagination
    const totalCount = userIds.length;
    const paginatedUserIds = userIds.slice((page - 1) * limit, page * limit);
    
    const users = await usersCollection.find({
      _id: { $in: paginatedUserIds.map(id => new ObjectId(id)) },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
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
    
    // Base query
    let query = {
      status: 'partial',
      remainingAmount: { $gt: 0 },
      $and: [
        {
          $or: [
            { serviceStatus: { $ne: 'inactive' } },
            { serviceStatus: { $exists: false } }
          ]
        }
      ]
    };
    
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
    const query = {
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    
    // Only filter by showInExpiringSoon flag when no specific date is requested
    if (!filterByDate) {
      query.showInExpiringSoon = true;
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
        // Show only if expiring today or tomorrow
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
app.get('/api/transactions/expense', ensureDbConnection, async (req, res) => {
  try {
    console.log('🔍 Database Name:', db.databaseName);
    console.log('🔍 Expected Database:', DB_NAME);
    console.log('🔍 MONGODB_URI exists:', !!MONGODB_URI);
    console.log('🔍 Checking expenses collection...');
    
    // Ensure we're using the correct database
    const correctDb = client.db(DB_NAME);
    console.log('🔍 Correct DB Name:', correctDb.databaseName);
    
    // List all collections to verify expenses exists
    const collections = await correctDb.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);
    console.log('📋 All collections in', DB_NAME, ':', collectionNames);
    
    const hasExpenses = collectionNames.includes('expenses');
    console.log('✅ Expenses collection exists:', hasExpenses);
    
    if (!hasExpenses) {
      return res.status(200).json({
        success: false,
        message: 'Expenses collection does not exist',
        database: correctDb.databaseName,
        expectedDatabase: DB_NAME,
        availableCollections: collectionNames
      });
    }
    
    // Count documents
    const count = await correctDb.collection('expenses').countDocuments();
    console.log('📊 Total documents in expenses:', count);
    
    // Fetch with detailed logging
    const result = await correctDb.collection('expenses').find({}).sort({ date: -1 }).toArray();
    console.log('✅ Fetched expenses:', result.length);
    
    if (result.length > 0) {
      console.log('📄 Sample expense:', JSON.stringify(result[0]));
    }
 
    res.status(200).json({
      success: true,
      count: result.length,
      data: result,
      debug: {
        database: correctDb.databaseName,
        expectedDatabase: DB_NAME
      }
    });
  } catch (error) {
    console.error('❌ Error fetching expenses:', error);
    res.status(500).json({
      success: false,
      message: 'Error fetching expenses',
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
    const { amount, description, category, paidTo, date } = req.body;
    
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
      date: date ? new Date(date) : new Date(),
      createdAt: new Date()
    };
    
    // Insert into expenses collection
    const result = await expensesCollection.insertOne(expense);
    
    // Get the created expense
    const createdExpense = await expensesCollection.findOne({ _id: result.insertedId });
    
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
      
      // Check if user already has a voucher
      const existingVoucher = await vouchersCollection.findOne({ userId });
      
      if (existingVoucher) {
        // Check if any months being paid have reversed status
        const refundsCollection = db.collection('refunds');
        const reversedMonthsPaid = months.filter(m => {
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
        
        // Update existing voucher with new months array
        const result = await vouchersCollection.updateOne(
          { userId },
          { 
            $set: { 
              months,
              rechargeDate: rechargeDate || existingVoucher.rechargeDate,
              expiryDate: expiryDate || existingVoucher.expiryDate
            } 
          }
        );
        
        return res.status(200).json({
          success: true,
          message: 'Voucher updated with months array',
          data: { _id: existingVoucher._id }
        });
      } else {
        // Create new voucher with months array
        const newVoucher = {
          userId,
          userName,
          rechargeDate: rechargeDate || null,
          expiryDate: expiryDate || null,
          months,
          createdAt: new Date()
        };
        
        const result = await vouchersCollection.insertOne(newVoucher);
        
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

    if (Array.isArray(months)) updateFields.months = months;
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
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
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
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
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
        
        // Calculate next month's expiry date
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
        const monthName = nextExpiryDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
        
        console.log(`   → Creating next month voucher: ${monthName}`);
        console.log(`   → New expiry date: ${newExpiryDateStr}`);
        
        // Update user: change to unpaid, update expiry date, remove from Expiring Soon
        await usersCollection.updateOne(
          { _id: user._id },
          { 
            $set: { 
              status: 'unpaid',
              expiryDate: newExpiryDateStr,
              showInExpiringSoon: false
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
          date: new Date(),
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
    const { brand, model, quantity, price, purchaseDate, status } = req.body;

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
    const { type, length, pricePerMeter, purchaseDate, supplier, status } = req.body;

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

// ============ TOTAL SALES API ROUTE ============

// GET total sales from routers and fiber cables
app.get('/api/sales/total', async (req, res) => {
  try {
    // Get all routers with sales
    const routers = await routersCollection.find({
      quantitySold: { $gt: 0 }
    }).toArray();
    
    // Calculate router sales
    let routerSales = 0;
    routers.forEach(router => {
      if (router.salesHistory && router.salesHistory.length > 0) {
        router.salesHistory.forEach(sale => {
          routerSales += sale.quantity * sale.sellingPrice;
        });
      }
    });
    
    // Get all fiber cables with sales
    const cables = await fiberCablesCollection.find({
      lengthSold: { $gt: 0 }
    }).toArray();
    
    // Calculate fiber cable sales
    let cableSales = 0;
    cables.forEach(cable => {
      if (cable.salesHistory && cable.salesHistory.length > 0) {
        cable.salesHistory.forEach(sale => {
          cableSales += sale.length * sale.sellingPricePerMeter;
        });
      }
    });
    
    const totalSales = routerSales + cableSales;
    
    res.status(200).json({
      success: true,
      data: {
        totalSales,
        routerSales,
        cableSales,
        breakdown: {
          routers: routerSales,
          fiberCables: cableSales
        }
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

// ============ VOUCHERS ROUTES ============
// Get all vouchers
app.get('/api/vouchers', ensureDbConnection, async (req, res) => {
  try {
    const vouchers = await vouchersCollection.find({}).toArray();
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

// Export for Vercel serverless
module.exports = app;
