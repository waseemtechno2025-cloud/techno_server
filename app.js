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
let isConnected = false;
let client;

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
      transactions: !!transactionsCollection
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
    console.log(`[${req.method}] ${req.path} - Connection status: ${isConnected}, DB: ${!!db}, Collections: ${!!usersCollection}`);
    
    if (!isConnected || !db || !usersCollection) {
      console.log('Database not connected, connecting now...');
      await connectToDatabase();
      console.log('Database connected successfully in middleware');
    }
    
    // Double check collections are initialized
    if (!usersCollection) {
      throw new Error('Collections failed to initialize');
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
    
    // Determine payment status based on paymentType or explicit status
    let paymentStatus = 'unpaid'; // Default for "Pay Later"
    let paidAmount = 0;
    let remainingAmount = totalAmountForAllMonths;
    
    // If status is explicitly set to 'pending', use that (for disable paid/unpaid checkbox)
    if (status === 'pending') {
      paymentStatus = 'pending';
      console.log('✅ Using pending status - customer will appear in Expiring Soon only');
    } else if (paymentType === 'now') {
      // For "Pay Now", first month is paid, remaining months are pending
      paymentStatus = numberOfMonths > 1 ? 'partial' : 'paid';
      paidAmount = monthlyFeeAfterDiscount; // Only first month paid
      remainingAmount = totalAmountForAllMonths - monthlyFeeAfterDiscount; // Remaining months
    }
    
    console.log('📊 Final Payment Status:', {
      paymentStatus,
      paidAmount,
      remainingAmount
    });

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
      createdAt: new Date()
    };

    const result = await usersCollection.insertOne(newUser);
    const newUserId = result.insertedId;
    
    // Voucher creation functionality has been removed from this endpoint
    // Vouchers will be created separately through the dedicated voucher endpoint
    
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
    
    const users = await usersCollection.find(query).sort({ createdAt: -1 }).toArray();
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
    const rechargeDate = req.query.paymentDate; // YYYY-MM-DD format (frontend sends as paymentDate but we filter by rechargeDate)
    
    let userIds = [];
    
    // If recharge date filter is provided, find users from vouchers collection
    if (rechargeDate) {
      const [year, month, day] = rechargeDate.split('-');
      const dateWithHyphen = `${day}-${month}-${year}`;
      const dateWithSlash = `${day}/${month}/${year}`;
      console.log(`Recharge date filter: ${rechargeDate} → ${dateWithHyphen} or ${dateWithSlash}`);
      
      // Find vouchers with matching recharge date
      const vouchers = await vouchersCollection.find({
        rechargeDate: { $in: [dateWithHyphen, dateWithSlash] }
      }).toArray();
      
      console.log(`📊 Query result: Found ${vouchers.length} vouchers`);
      vouchers.forEach(v => {
        console.log(`  - User: ${v.userName}, rechargeDate: ${v.rechargeDate}, expiryDate: ${v.expiryDate}`);
      });
      
      userIds = vouchers.map(v => v.userId);
      console.log(`Found ${userIds.length} vouchers with recharge date ${rechargeDate}`);
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
    if (rechargeDate && userIds.length > 0) {
      const objectIds = userIds.map(id => new ObjectId(id));
      query._id = { $in: objectIds };
      console.log(`🔍 Filtering users with IDs:`, objectIds.map(id => id.toString()));
    } else if (rechargeDate && userIds.length === 0) {
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
      .sort({ createdAt: -1 })
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
    
    let userIds = [];
    
    // If expiry date filter is provided, find users from vouchers collection
    if (expiryDate) {
      const [year, month, day] = expiryDate.split('-');
      const dateWithHyphen = `${day}-${month}-${year}`;
      const dateWithSlash = `${day}/${month}/${year}`;
      console.log(`Date filter: ${expiryDate} → ${dateWithHyphen} or ${dateWithSlash}`);
      
      // Find vouchers with matching expiry date
      const vouchers = await vouchersCollection.find({
        expiryDate: { $in: [dateWithHyphen, dateWithSlash] }
      }).toArray();
      
      userIds = vouchers.map(v => v.userId);
      console.log(`Found ${userIds.length} vouchers with expiry date ${expiryDate}`);
    }
    
    // Base query - Only show unpaid status (exclude pending)
    let query = {
      status: 'unpaid',
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    
    // Add user ID filter if expiry date was provided
    if (expiryDate && userIds.length > 0) {
      query._id = { $in: userIds.map(id => new ObjectId(id)) };
    } else if (expiryDate && userIds.length === 0) {
      // No users found for this expiry date
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
      .sort({ createdAt: -1 })
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

// GET balance users (partial payment users with date filter and pagination)
app.get('/api/balances', async (req, res) => {
  try {
    const usersCollection = db.collection('users');
    const vouchersCollection = db.collection('vouchers');
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const expiryDate = req.query.expiryDate; // YYYY-MM-DD format
    
    let userIds = [];
    
    // If expiry date filter is provided, find users from vouchers collection
    if (expiryDate) {
      const [year, month, day] = expiryDate.split('-');
      const dateWithHyphen = `${day}-${month}-${year}`;
      const dateWithSlash = `${day}/${month}/${year}`;
      console.log(`Date filter: ${expiryDate} → ${dateWithHyphen} or ${dateWithSlash}`);
      
      // Find vouchers with matching expiry date
      const vouchers = await vouchersCollection.find({
        expiryDate: { $in: [dateWithHyphen, dateWithSlash] }
      }).toArray();
      
      userIds = vouchers.map(v => v.userId);
      console.log(`Found ${userIds.length} vouchers with expiry date ${expiryDate}`);
    }
    
    // Base query
    let query = {
      status: 'partial',
      remainingAmount: { $gt: 0 },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    };
    
    // Add user ID filter if expiry date was provided
    if (expiryDate && userIds.length > 0) {
      query._id = { $in: userIds.map(id => new ObjectId(id)) };
    } else if (expiryDate && userIds.length === 0) {
      // No users found for this expiry date
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
      .sort({ createdAt: -1 })
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
    const PKT_OFFSET_MIN = 5 * 60;
    const nowUTC = new Date();
    const nowInPKT = new Date(nowUTC.getTime() + PKT_OFFSET_MIN * 60000);
    const todayY = nowInPKT.getUTCFullYear();
    const todayM = nowInPKT.getUTCMonth();
    const todayD = nowInPKT.getUTCDate();
    
    // Determine target calendar day (PKT) from query or default to tomorrow
    const dateParam = req.query.date; // YYYY-MM-DD
    let targetY, targetM, targetD;
    if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(String(dateParam))) {
      const [yyyy, mm, dd] = String(dateParam).split('-');
      targetY = parseInt(yyyy, 10);
      targetM = parseInt(mm, 10) - 1; // JS months 0-based
      targetD = parseInt(dd, 10);
    } else {
      const tomorrowInPKT = new Date(Date.UTC(todayY, todayM, todayD) + 24 * 60 * 60 * 1000);
      targetY = tomorrowInPKT.getUTCFullYear();
      targetM = tomorrowInPKT.getUTCMonth();
      targetD = tomorrowInPKT.getUTCDate();
    }

    // Fetch ALL users (paid/partial/unpaid/pending) and non-inactive users, filter expiry by JS supporting string dates
    // NOTE: Include unpaid and pending so "Pay Later" and checkbox users appear in expiring soon based on date
    const usersAll = await usersCollection.find({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
      $or: [
        { serviceStatus: { $ne: 'inactive' } },
        { serviceStatus: { $exists: false } }
      ]
    }).toArray();

    // Helper: parse expiryDate to PKT Y/M/D
    const toPKT_YMD = (dateObj) => {
      const pkt = new Date(dateObj.getTime() + PKT_OFFSET_MIN * 60000);
      return { y: pkt.getUTCFullYear(), m: pkt.getUTCMonth(), d: pkt.getUTCDate() };
    };
    const parseExpiryYMD = (exp) => {
      if (!exp) return null;
      if (exp instanceof Date) {
        return toPKT_YMD(exp);
      }
      if (typeof exp === 'string') {
        const parts = exp.split('-');
        if (parts.length === 3) {
          const [dd, mm, yyyy] = parts;
          const d = parseInt(dd, 10);
          const m = parseInt(mm, 10) - 1;
          const y = parseInt(yyyy, 10);
          if (!isNaN(d) && !isNaN(m) && !isNaN(y)) {
            // Build a UTC Date for that calendar day, then convert to PKT YMD
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

    const filtered = usersAll
      .map(u => ({ u, ymd: parseExpiryYMD(u.expiryDate) }))
      .filter(({ ymd }) => ymd && ymd.y === targetY && ymd.m === targetM && ymd.d === targetD)
      .sort((a, b) => {
        // Sort by calendar day
        if (!a.ymd || !b.ymd) return 0;
        if (a.ymd.y !== b.ymd.y) return a.ymd.y - b.ymd.y;
        if (a.ymd.m !== b.ymd.m) return a.ymd.m - b.ymd.m;
        return a.ymd.d - b.ymd.d;
      });

    // Calculate days left using PKT midnights
    const MS_PER_DAY = 24 * 60 * 60 * 1000;
    const todayPKTMidUTC = Date.UTC(todayY, todayM, todayD) - PKT_OFFSET_MIN * 60000;
    const usersWithDaysLeft = filtered.map(({ u, ymd }) => {
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

// GET expense transactions
app.get('/api/transactions/expense', async (req, res) => {
  try {
    const transactionsCollection = db.collection('transactions');
    const transactions = await transactionsCollection.find({
      type: 'expense'
    }).sort({ date: -1 }).toArray();
    
    res.status(200).json({
      success: true,
      count: transactions.length,
      data: transactions
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
    }).sort({ createdAt: 1 }).toArray();

    // Build transaction history
    const transactions = [];
    let runningBalance = 0;

    vouchers.forEach(voucher => {
      const date = new Date(voucher.createdAt);
      const monthYear = date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
      
      // Add package fee as debit
      runningBalance -= voucher.packageFee;
      transactions.push({
        date: monthYear,
        description: `${date.toLocaleDateString('en-GB', { month: 'long' })} Fee`,
        debit: voucher.packageFee,
        credit: null,
        balance: runningBalance
      });

      // Add payment as credit (if paid)
      if (voucher.paidAmount > 0) {
        runningBalance += voucher.paidAmount;
        transactions.push({
          date: monthYear,
          description: 'Payment Received',
          debit: null,
          credit: voucher.paidAmount,
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

// Function to check users expiring TOMORROW (next day) - shows in expiring-soon tab
const checkTomorrowExpiringUsers = async () => {
  try {
    console.log('🕐 Running scheduled task: Checking users expiring TOMORROW...');
    
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    // Calculate tomorrow's date
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const endOfTomorrow = new Date(tomorrow);
    endOfTomorrow.setHours(23, 59, 59, 999);
    
    // Find users who are paid/partial/unpaid/pending and expiring TOMORROW
    // NOTE: Include unpaid and pending so "Pay Later" and checkbox users are also checked
    const expiringTomorrowUsers = await usersCollection.find({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
      expiryDate: { 
        $gte: tomorrow.toISOString(), 
        $lte: endOfTomorrow.toISOString() 
      }
    }).toArray();
    
    console.log(`✅ Found ${expiringTomorrowUsers.length} users expiring TOMORROW (${tomorrow.toDateString()})`);
    
    if (expiringTomorrowUsers.length > 0) {
      expiringTomorrowUsers.forEach(user => {
        console.log(`   - ${user.userName} expires TOMORROW (${user.expiryDate})`);
      });
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

// Function to move TODAY's expiring users to unpaid status and create next month's voucher
// RECURRING CYCLE FLOW:
// 1. User added: 28/10/2025 (recharge) → 28/11/2025 (expiry) → Status: PAID
// 2. On 27/11/2025: User shows in "Expiring Soon" (1 day before expiry)
// 3. On 28/11/2025 (TODAY): This function runs at noon (12:00 PM)
//    - User moves to UNPAID status
//    - Creates December voucher (unpaid)
//    - Updates expiry to 28/12/2025
// 4. User pays December: Status → PAID, shows in paid-users
// 5. On 27/12/2025: Shows in "Expiring Soon" again
// 6. On 28/12/2025: Repeats cycle (creates January voucher, expiry → 28/01/2026)
// 7. Transaction history shows all months: November, December, January, etc.
const moveTodayExpiredToUnpaid = async () => {
  try {
    console.log('🕐 Running scheduled task: Moving TODAY expiring users to unpaid...');
    
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const endOfToday = new Date(today);
    endOfToday.setHours(23, 59, 59, 999);
    
    // Find ALL users (paid/partial/unpaid/pending) expiring TODAY
    // NOTE: Include unpaid and pending so "Pay Later" and checkbox users also get next month's voucher
    const expiredUsers = await usersCollection.find({
      status: { $in: ['paid', 'partial', 'unpaid', 'pending'] },
      expiryDate: { 
        $gte: today.toISOString(),
        $lte: endOfToday.toISOString() 
      }
    }).toArray();
    
    console.log(`✅ Found ${expiredUsers.length} users expiring today (will create next month voucher)`);
    
    if (expiredUsers.length > 0) {
      // Process each user: move to unpaid and create next month's voucher
      for (const user of expiredUsers) {
        console.log(`   - Processing ${user.userName} (expired: ${user.expiryDate})`);
        
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
        
        // Update user status to unpaid and new expiry date
        await usersCollection.updateOne(
          { _id: user._id },
          { 
            $set: { 
              status: 'unpaid',
              expiryDate: newExpiryDateStr
            } 
          }
        );
        
        // Find or create voucher for this user
        let userVoucher = await vouchersCollection.findOne({ userId: user._id.toString() });
        
        const newMonth = {
          month: monthName,
          packageFee: user.amount || 0,
          paidAmount: 0,
          remainingAmount: user.amount || 0,
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
                  rechargeDate: user.rechargeDate // Keep original recharge date
                }
              }
            );
            
            console.log(`   ✅ Added ${monthName} to voucher`);
          } else {
            console.log(`   ⚠️ ${monthName} already exists in voucher`);
          }
        } else {
          // Create new voucher document for this user (especially for pending users)
          console.log(`   📝 Creating new voucher for ${user.userName}`);
          
          const newVoucher = {
            userId: user._id.toString(),
            userName: user.userName,
            rechargeDate: user.rechargeDate || formatDate(currentExpiryDate),
            expiryDate: newExpiryDateStr,
            packageFee: user.amount || 0,
            paidAmount: 0,
            remainingAmount: user.amount || 0,
            paymentMethod: 'Pending',
            receivedBy: '',
            paymentType: 'later',
            status: 'unpaid',
            months: [newMonth],
            createdAt: new Date()
          };
          
          await vouchersCollection.insertOne(newVoucher);
          console.log(`   ✅ Created new voucher with ${monthName}`);
        }
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
  // ===== MAIN PRODUCTION TASKS - Run at Noon (12:00 PM) =====
  
  // Schedule task: Daily at noon - Check users expiring TOMORROW + Move TODAY expiring to unpaid
  cron.schedule('0 12 * * *', () => {
    console.log('⏰ Noon (12:00 PM) task triggered');
    
    // First: Check users expiring TOMORROW (for expiring-soon tab)
    checkTomorrowExpiringUsers();
    
    // Second: Move TODAY's expiring users to unpaid (from expiring-soon to unpaid)
    moveTodayExpiredToUnpaid();
  }, {
    timezone: "Asia/Karachi"
  });

  // ===== PAYMENT REMINDER TASK - Run at 8:00 PM daily =====
  cron.schedule('0 20 * * *', () => {
    console.log('⏰ 8:00 PM Reminder task triggered');
    checkAndSendReminders();
  }, {
    timezone: "Asia/Karachi"
  });

  // Run once on server start to check immediately
  console.log('🔄 Running initial checks on server start...');
  checkTomorrowExpiringUsers();
  moveTodayExpiredToUnpaid();
  
  // Check for missed reminders on server startup
  checkMissedReminders();

  console.log('📅 Scheduled tasks initialized:');
  console.log('   → 12:00 PM (noon): Check expiring users & move expired to unpaid');
  console.log('   → 8:00 PM: Check and send payment reminders');
  console.log('   → On startup: Check and send any missed reminders');
};

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

// Export for Vercel serverless
module.exports = app;
