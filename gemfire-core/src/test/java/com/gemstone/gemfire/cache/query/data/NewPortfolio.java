/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.data;

import java.io.Serializable;
import java.util.*;

/**
 * A version of the Portfolio Object used for query. 
 */
public class NewPortfolio implements Serializable {
  
  protected String myVersion;

  protected static final Random rng = new Random();

  protected int NUM_OF_TYPES = 10;
  protected int MAX_NUM_OF_POSITIONS = 5;     
  protected int NUM_OF_SECURITIES = 200;
  private int MAX_QTY = 100;    //max is 100*100 
  private int MAX_PRICE = 100;
  protected int id = 0;           
  protected String name = "name";         //key value, needs to be unique
  protected String status = "status";
  protected String type = "type";
  protected Map positions = new HashMap();
  public String undefinedTestField = null;
  
  public NewPortfolio() {
    //use default
    myVersion = "tests/parReg.query.NewPortfolio";
  }
  
  /**
   * Constructor to randomly populate the portfolio.
   * @param name
   * @param id
   */
  public NewPortfolio(String name, int id) {
    myVersion = "tests/parReg.query.NewPortfolio";
    this.name = name;
    this.id = id;
    
    this.status = id % 2 == 0 ? "active" : "inactive";
    this.type = "type" + (id % NUM_OF_TYPES);
    
    setPositions();
  }
  
  public int getId() {
    return id;
  }
  
  public String getName() {
    return name;
  }
  
  public String getStatus() {
    return status;
  }
  
  public String getType() {
    return type;
  }
  
  public void setId(int id) {
    this.id = id;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void  setStatus(String status) {
    this.status = status;
  }
  
  public void setType(String type) {
    this.type = type;
  }
    
  public void init( int i ) {
    this.name = new Integer(i).toString();
    this.id = i;
    this.status = i % 2 == 0 ? "active" : "inactive";
    this.type = "type" + (i % NUM_OF_TYPES);
    
    setPositions();

  }
  
  private void setPositions() {
    int numOfPositions = rng.nextInt(MAX_NUM_OF_POSITIONS);
    if (numOfPositions == 0) 
      numOfPositions++;
     
    int secId =  rng.nextInt(NUM_OF_SECURITIES);
    
    for (int i=0; i < numOfPositions; i++) {
      Properties props = getProps();
      
//    secId needs to be UNIQUE in one portfolio, keep track MAX_NUM_OF_POSITIONS and NUM_OF_SECURITIES
      secId += i * 7;                    
      if (secId > NUM_OF_SECURITIES)
        secId -= NUM_OF_SECURITIES;
      props.setProperty("secId", new Integer(secId).toString());
      
      NewPosition pos = new NewPosition();
      pos.init(props);
      this.positions.put(pos.getSecId(), pos);
    }
  }
  
  public void validate( int index ) {
    //do nothing
  }
  
  public int getIndex() {
    return this.id;
  }
  
  public Map getPositions(){
    return positions;
  }
  
  /**
   * To provide random values to populate a position.
   * @return
   */
  protected Properties getProps() {
   Properties props = new Properties();
   Double qty = new Double(rng.nextInt(MAX_QTY) * 100.00);
   Double mktValue = new Double(rng.nextDouble() * MAX_PRICE); 

   props.setProperty("qty", qty.toString());
   props.setProperty("mktValue", mktValue.toString());

   return props;
  }
  
  /**
   * To enable the comparison.
   */
  public boolean equals(Object anObj) {
    
    if (anObj == null) {
       return false;
    }

    if (anObj.getClass().getName().equals(this.getClass().getName())) { // cannot do class identity check for pdx tets
       NewPortfolio np = (NewPortfolio)anObj;
       if (!np.name.equals(this.name) || (np.id != this.id) || !np.type.equals(this.type) || !np.status.equals(this.status)) {
         return false;
       }
       
       if (np.positions == null) {
          if (this.positions != null) {
            return false;
          }
       } else {
         if (np.positions.size() != this.positions.size()) {
           return false;
         }
         else {                 //loops thru the map of positions
           Iterator itr = np.positions.values().iterator();
           NewPosition pos;
           while (itr.hasNext()) {
             pos = (NewPosition)itr.next();
             if (!this.positions.containsValue(pos)){
               return false;
             }            
           }
         }
       }
    } else {
      //not same class
       return false;
    }
    return true;
 }

  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + status.hashCode();
    result = 37 * result + type.hashCode();
    result = 37 * result + id;
    result = 37 * result + positions.hashCode();
    
    return result;
  }
 
  /** Create a map of fields and field values to use to write to the blackboard
   *  since PdxSerialiables cannot be put on the blackboard since the MasterController
   *  does not have pdx objects on its classpath. For PdxSerializables
   *  we put this Map on the blackboard instead.
   */
  public Map createPdxHelperMap() {
    Map fieldMap = new HashMap();
    fieldMap.put("className", this.getClass().getName());
    fieldMap.put("myVersion", myVersion);
    fieldMap.put("id", id);
    fieldMap.put("name", name);
    fieldMap.put("status", status);
    fieldMap.put("type", type);
    fieldMap.put("positions", positions);
    fieldMap.put("undefinedTestField", undefinedTestField);
    return fieldMap;
  }

  /** Restore the fields of this instance using the values of the Map, created
   *  by createPdxHelperMap()
   */
  public void restoreFromPdxHelperMap(Map aMap) {
    this.myVersion = (String)aMap.get("myVersion");
    this.id = (Integer)aMap.get("id");
    this.name = (String)aMap.get("name");
    this.status = (String)aMap.get("status");
    this.type = (String)aMap.get("type");
    this.positions = (Map)aMap.get("positions");
    this.undefinedTestField = (String)aMap.get("undefinedTestField");
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("NewPortfolio [ID=" + this.id + " status=" + status);
    sb.append(" name=" + this.name);
    
    Iterator iter = positions.entrySet().iterator();
    sb.append(" NewPositions:[ ");
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      sb.append(entry.getKey() + ":" + entry.getValue() + ", ");
    }
    sb.append("] ]");
    return sb.toString();
  }

}

