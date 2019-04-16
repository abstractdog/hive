/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class PartitionsByExprResult implements org.apache.thrift.TBase<PartitionsByExprResult, PartitionsByExprResult._Fields>, java.io.Serializable, Cloneable, Comparable<PartitionsByExprResult> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PartitionsByExprResult");

  private static final org.apache.thrift.protocol.TField PARTITIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("partitions", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField HAS_UNKNOWN_PARTITIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("hasUnknownPartitions", org.apache.thrift.protocol.TType.BOOL, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new PartitionsByExprResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new PartitionsByExprResultTupleSchemeFactory());
  }

  private List<Partition> partitions; // required
  private boolean hasUnknownPartitions; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PARTITIONS((short)1, "partitions"),
    HAS_UNKNOWN_PARTITIONS((short)2, "hasUnknownPartitions");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PARTITIONS
          return PARTITIONS;
        case 2: // HAS_UNKNOWN_PARTITIONS
          return HAS_UNKNOWN_PARTITIONS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __HASUNKNOWNPARTITIONS_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PARTITIONS, new org.apache.thrift.meta_data.FieldMetaData("partitions", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Partition.class))));
    tmpMap.put(_Fields.HAS_UNKNOWN_PARTITIONS, new org.apache.thrift.meta_data.FieldMetaData("hasUnknownPartitions", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PartitionsByExprResult.class, metaDataMap);
  }

  public PartitionsByExprResult() {
  }

  public PartitionsByExprResult(
    List<Partition> partitions,
    boolean hasUnknownPartitions)
  {
    this();
    this.partitions = partitions;
    this.hasUnknownPartitions = hasUnknownPartitions;
    setHasUnknownPartitionsIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PartitionsByExprResult(PartitionsByExprResult other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetPartitions()) {
      List<Partition> __this__partitions = new ArrayList<Partition>(other.partitions.size());
      for (Partition other_element : other.partitions) {
        __this__partitions.add(new Partition(other_element));
      }
      this.partitions = __this__partitions;
    }
    this.hasUnknownPartitions = other.hasUnknownPartitions;
  }

  public PartitionsByExprResult deepCopy() {
    return new PartitionsByExprResult(this);
  }

  @Override
  public void clear() {
    this.partitions = null;
    setHasUnknownPartitionsIsSet(false);
    this.hasUnknownPartitions = false;
  }

  public int getPartitionsSize() {
    return (this.partitions == null) ? 0 : this.partitions.size();
  }

  public java.util.Iterator<Partition> getPartitionsIterator() {
    return (this.partitions == null) ? null : this.partitions.iterator();
  }

  public void addToPartitions(Partition elem) {
    if (this.partitions == null) {
      this.partitions = new ArrayList<Partition>();
    }
    this.partitions.add(elem);
  }

  public List<Partition> getPartitions() {
    return this.partitions;
  }

  public void setPartitions(List<Partition> partitions) {
    this.partitions = partitions;
  }

  public void unsetPartitions() {
    this.partitions = null;
  }

  /** Returns true if field partitions is set (has been assigned a value) and false otherwise */
  public boolean isSetPartitions() {
    return this.partitions != null;
  }

  public void setPartitionsIsSet(boolean value) {
    if (!value) {
      this.partitions = null;
    }
  }

  public boolean isHasUnknownPartitions() {
    return this.hasUnknownPartitions;
  }

  public void setHasUnknownPartitions(boolean hasUnknownPartitions) {
    this.hasUnknownPartitions = hasUnknownPartitions;
    setHasUnknownPartitionsIsSet(true);
  }

  public void unsetHasUnknownPartitions() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __HASUNKNOWNPARTITIONS_ISSET_ID);
  }

  /** Returns true if field hasUnknownPartitions is set (has been assigned a value) and false otherwise */
  public boolean isSetHasUnknownPartitions() {
    return EncodingUtils.testBit(__isset_bitfield, __HASUNKNOWNPARTITIONS_ISSET_ID);
  }

  public void setHasUnknownPartitionsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __HASUNKNOWNPARTITIONS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PARTITIONS:
      if (value == null) {
        unsetPartitions();
      } else {
        setPartitions((List<Partition>)value);
      }
      break;

    case HAS_UNKNOWN_PARTITIONS:
      if (value == null) {
        unsetHasUnknownPartitions();
      } else {
        setHasUnknownPartitions((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PARTITIONS:
      return getPartitions();

    case HAS_UNKNOWN_PARTITIONS:
      return isHasUnknownPartitions();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PARTITIONS:
      return isSetPartitions();
    case HAS_UNKNOWN_PARTITIONS:
      return isSetHasUnknownPartitions();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof PartitionsByExprResult)
      return this.equals((PartitionsByExprResult)that);
    return false;
  }

  public boolean equals(PartitionsByExprResult that) {
    if (that == null)
      return false;

    boolean this_present_partitions = true && this.isSetPartitions();
    boolean that_present_partitions = true && that.isSetPartitions();
    if (this_present_partitions || that_present_partitions) {
      if (!(this_present_partitions && that_present_partitions))
        return false;
      if (!this.partitions.equals(that.partitions))
        return false;
    }

    boolean this_present_hasUnknownPartitions = true;
    boolean that_present_hasUnknownPartitions = true;
    if (this_present_hasUnknownPartitions || that_present_hasUnknownPartitions) {
      if (!(this_present_hasUnknownPartitions && that_present_hasUnknownPartitions))
        return false;
      if (this.hasUnknownPartitions != that.hasUnknownPartitions)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_partitions = true && (isSetPartitions());
    list.add(present_partitions);
    if (present_partitions)
      list.add(partitions);

    boolean present_hasUnknownPartitions = true;
    list.add(present_hasUnknownPartitions);
    if (present_hasUnknownPartitions)
      list.add(hasUnknownPartitions);

    return list.hashCode();
  }

  @Override
  public int compareTo(PartitionsByExprResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetPartitions()).compareTo(other.isSetPartitions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartitions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partitions, other.partitions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetHasUnknownPartitions()).compareTo(other.isSetHasUnknownPartitions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHasUnknownPartitions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hasUnknownPartitions, other.hasUnknownPartitions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PartitionsByExprResult(");
    boolean first = true;

    sb.append("partitions:");
    if (this.partitions == null) {
      sb.append("null");
    } else {
      sb.append(this.partitions);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("hasUnknownPartitions:");
    sb.append(this.hasUnknownPartitions);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetPartitions()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'partitions' is unset! Struct:" + toString());
    }

    if (!isSetHasUnknownPartitions()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hasUnknownPartitions' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class PartitionsByExprResultStandardSchemeFactory implements SchemeFactory {
    public PartitionsByExprResultStandardScheme getScheme() {
      return new PartitionsByExprResultStandardScheme();
    }
  }

  private static class PartitionsByExprResultStandardScheme extends StandardScheme<PartitionsByExprResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, PartitionsByExprResult struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PARTITIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list432 = iprot.readListBegin();
                struct.partitions = new ArrayList<Partition>(_list432.size);
                Partition _elem433;
                for (int _i434 = 0; _i434 < _list432.size; ++_i434)
                {
                  _elem433 = new Partition();
                  _elem433.read(iprot);
                  struct.partitions.add(_elem433);
                }
                iprot.readListEnd();
              }
              struct.setPartitionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // HAS_UNKNOWN_PARTITIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.hasUnknownPartitions = iprot.readBool();
              struct.setHasUnknownPartitionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, PartitionsByExprResult struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.partitions != null) {
        oprot.writeFieldBegin(PARTITIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.partitions.size()));
          for (Partition _iter435 : struct.partitions)
          {
            _iter435.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(HAS_UNKNOWN_PARTITIONS_FIELD_DESC);
      oprot.writeBool(struct.hasUnknownPartitions);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class PartitionsByExprResultTupleSchemeFactory implements SchemeFactory {
    public PartitionsByExprResultTupleScheme getScheme() {
      return new PartitionsByExprResultTupleScheme();
    }
  }

  private static class PartitionsByExprResultTupleScheme extends TupleScheme<PartitionsByExprResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, PartitionsByExprResult struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.partitions.size());
        for (Partition _iter436 : struct.partitions)
        {
          _iter436.write(oprot);
        }
      }
      oprot.writeBool(struct.hasUnknownPartitions);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, PartitionsByExprResult struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list437 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.partitions = new ArrayList<Partition>(_list437.size);
        Partition _elem438;
        for (int _i439 = 0; _i439 < _list437.size; ++_i439)
        {
          _elem438 = new Partition();
          _elem438.read(iprot);
          struct.partitions.add(_elem438);
        }
      }
      struct.setPartitionsIsSet(true);
      struct.hasUnknownPartitions = iprot.readBool();
      struct.setHasUnknownPartitionsIsSet(true);
    }
  }

}

