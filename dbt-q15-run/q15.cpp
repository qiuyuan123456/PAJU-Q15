#define USE_OLD_MAP
#include "program_base.hpp"
#include "hpds/KDouble.hpp"
#include "hash.hpp"
#include "mmap/mmap.hpp"
#include "hpds/pstring.hpp"
#include "hpds/pstringops.hpp"



#define RELATION_LINEITEM_DYNAMIC
#define RELATION_SUPPLIER_DYNAMIC

namespace dbtoaster {

  /* Definitions of maps used for storing materialized views. */
  struct COUNT_entry {
    long S_SUPPKEY; STRING_TYPE S_NAME; STRING_TYPE S_ADDRESS; STRING_TYPE S_PHONE; DOUBLE_TYPE R1_TOTAL_REVENUE; long __av; COUNT_entry* nxt; COUNT_entry* prv;
  
    explicit COUNT_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_entry(const long c0, const STRING_TYPE& c1, const STRING_TYPE& c2, const STRING_TYPE& c3, const DOUBLE_TYPE c4, const long c5) { S_SUPPKEY = c0; S_NAME = c1; S_ADDRESS = c2; S_PHONE = c3; R1_TOTAL_REVENUE = c4; __av = c5;  }
    COUNT_entry(const COUNT_entry& other) : S_SUPPKEY(other.S_SUPPKEY), S_NAME(other.S_NAME), S_ADDRESS(other.S_ADDRESS), S_PHONE(other.S_PHONE), R1_TOTAL_REVENUE(other.R1_TOTAL_REVENUE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_entry& modify(const long c0, const STRING_TYPE& c1, const STRING_TYPE& c2, const STRING_TYPE& c3, const DOUBLE_TYPE c4) { S_SUPPKEY = c0; S_NAME = c1; S_ADDRESS = c2; S_PHONE = c3; R1_TOTAL_REVENUE = c4;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_NAME);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_ADDRESS);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_PHONE);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, R1_TOTAL_REVENUE);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mapkey01234_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_entry& e) {
      size_t h = 0;
      hash_combine(h, e.S_SUPPKEY);
      hash_combine(h, e.S_NAME);
      hash_combine(h, e.S_ADDRESS);
      hash_combine(h, e.S_PHONE);
      hash_combine(h, e.R1_TOTAL_REVENUE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_entry& x, const COUNT_entry& y) {
      return x.S_SUPPKEY == y.S_SUPPKEY && x.S_NAME == y.S_NAME && x.S_ADDRESS == y.S_ADDRESS && x.S_PHONE == y.S_PHONE && x.R1_TOTAL_REVENUE == y.R1_TOTAL_REVENUE;
    }
  };
  
  typedef MultiHashMap<COUNT_entry, long,
    HashIndex<COUNT_entry, long, COUNT_mapkey01234_idxfn, true>
  > COUNT_map;
  typedef HashIndex<COUNT_entry, long, COUNT_mapkey01234_idxfn, true> HashIndex_COUNT_map_01234;
  
  
  struct COUNT_mLINEITEM1_entry {
    long S_SUPPKEY; STRING_TYPE S_NAME; STRING_TYPE S_ADDRESS; STRING_TYPE S_PHONE; long __av; COUNT_mLINEITEM1_entry* nxt; COUNT_mLINEITEM1_entry* prv;
  
    explicit COUNT_mLINEITEM1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_mLINEITEM1_entry(const long c0, const STRING_TYPE& c1, const STRING_TYPE& c2, const STRING_TYPE& c3, const long c4) { S_SUPPKEY = c0; S_NAME = c1; S_ADDRESS = c2; S_PHONE = c3; __av = c4;  }
    COUNT_mLINEITEM1_entry(const COUNT_mLINEITEM1_entry& other) : S_SUPPKEY(other.S_SUPPKEY), S_NAME(other.S_NAME), S_ADDRESS(other.S_ADDRESS), S_PHONE(other.S_PHONE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_mLINEITEM1_entry& modify(const long c0, const STRING_TYPE& c1, const STRING_TYPE& c2, const STRING_TYPE& c3) { S_SUPPKEY = c0; S_NAME = c1; S_ADDRESS = c2; S_PHONE = c3;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_NAME);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_ADDRESS);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_PHONE);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mLINEITEM1_mapkey0123_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_mLINEITEM1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.S_SUPPKEY);
      hash_combine(h, e.S_NAME);
      hash_combine(h, e.S_ADDRESS);
      hash_combine(h, e.S_PHONE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_mLINEITEM1_entry& x, const COUNT_mLINEITEM1_entry& y) {
      return x.S_SUPPKEY == y.S_SUPPKEY && x.S_NAME == y.S_NAME && x.S_ADDRESS == y.S_ADDRESS && x.S_PHONE == y.S_PHONE;
    }
  };
  
  typedef MultiHashMap<COUNT_mLINEITEM1_entry, long,
    HashIndex<COUNT_mLINEITEM1_entry, long, COUNT_mLINEITEM1_mapkey0123_idxfn, true>
  > COUNT_mLINEITEM1_map;
  typedef HashIndex<COUNT_mLINEITEM1_entry, long, COUNT_mLINEITEM1_mapkey0123_idxfn, true> HashIndex_COUNT_mLINEITEM1_map_0123;
  
  
  struct COUNT_mLINEITEM1_E2_1_entry {
    long S_SUPPKEY; long __av; COUNT_mLINEITEM1_E2_1_entry* nxt; COUNT_mLINEITEM1_E2_1_entry* prv;
  
    explicit COUNT_mLINEITEM1_E2_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_mLINEITEM1_E2_1_entry(const long c0, const long c1) { S_SUPPKEY = c0; __av = c1;  }
    COUNT_mLINEITEM1_E2_1_entry(const COUNT_mLINEITEM1_E2_1_entry& other) : S_SUPPKEY(other.S_SUPPKEY), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_mLINEITEM1_E2_1_entry& modify(const long c0) { S_SUPPKEY = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mLINEITEM1_E2_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_mLINEITEM1_E2_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.S_SUPPKEY);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_mLINEITEM1_E2_1_entry& x, const COUNT_mLINEITEM1_E2_1_entry& y) {
      return x.S_SUPPKEY == y.S_SUPPKEY;
    }
  };
  
  typedef MultiHashMap<COUNT_mLINEITEM1_E2_1_entry, long,
    HashIndex<COUNT_mLINEITEM1_E2_1_entry, long, COUNT_mLINEITEM1_E2_1_mapkey0_idxfn, true>
  > COUNT_mLINEITEM1_E2_1_map;
  typedef HashIndex<COUNT_mLINEITEM1_E2_1_entry, long, COUNT_mLINEITEM1_E2_1_mapkey0_idxfn, true> HashIndex_COUNT_mLINEITEM1_E2_1_map_0;
  
  
  struct COUNT_mLINEITEM1_L3_1_entry {
    long S_SUPPKEY; DOUBLE_TYPE __av; COUNT_mLINEITEM1_L3_1_entry* nxt; COUNT_mLINEITEM1_L3_1_entry* prv;
  
    explicit COUNT_mLINEITEM1_L3_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_mLINEITEM1_L3_1_entry(const long c0, const DOUBLE_TYPE c1) { S_SUPPKEY = c0; __av = c1;  }
    COUNT_mLINEITEM1_L3_1_entry(const COUNT_mLINEITEM1_L3_1_entry& other) : S_SUPPKEY(other.S_SUPPKEY), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_mLINEITEM1_L3_1_entry& modify(const long c0) { S_SUPPKEY = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, S_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mLINEITEM1_L3_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_mLINEITEM1_L3_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.S_SUPPKEY);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_mLINEITEM1_L3_1_entry& x, const COUNT_mLINEITEM1_L3_1_entry& y) {
      return x.S_SUPPKEY == y.S_SUPPKEY;
    }
  };
  
  typedef MultiHashMap<COUNT_mLINEITEM1_L3_1_entry, DOUBLE_TYPE,
    HashIndex<COUNT_mLINEITEM1_L3_1_entry, DOUBLE_TYPE, COUNT_mLINEITEM1_L3_1_mapkey0_idxfn, true>
  > COUNT_mLINEITEM1_L3_1_map;
  typedef HashIndex<COUNT_mLINEITEM1_L3_1_entry, DOUBLE_TYPE, COUNT_mLINEITEM1_L3_1_mapkey0_idxfn, true> HashIndex_COUNT_mLINEITEM1_L3_1_map_0;
  
  
  struct COUNT_mLINEITEM1_L4_1_E1_1_entry {
    long R2_SUPPKEY; long __av; COUNT_mLINEITEM1_L4_1_E1_1_entry* nxt; COUNT_mLINEITEM1_L4_1_E1_1_entry* prv;
  
    explicit COUNT_mLINEITEM1_L4_1_E1_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_mLINEITEM1_L4_1_E1_1_entry(const long c0, const long c1) { R2_SUPPKEY = c0; __av = c1;  }
    COUNT_mLINEITEM1_L4_1_E1_1_entry(const COUNT_mLINEITEM1_L4_1_E1_1_entry& other) : R2_SUPPKEY(other.R2_SUPPKEY), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_mLINEITEM1_L4_1_E1_1_entry& modify(const long c0) { R2_SUPPKEY = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, R2_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mLINEITEM1_L4_1_E1_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_mLINEITEM1_L4_1_E1_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.R2_SUPPKEY);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_mLINEITEM1_L4_1_E1_1_entry& x, const COUNT_mLINEITEM1_L4_1_E1_1_entry& y) {
      return x.R2_SUPPKEY == y.R2_SUPPKEY;
    }
  };
  
  typedef MultiHashMap<COUNT_mLINEITEM1_L4_1_E1_1_entry, long,
    HashIndex<COUNT_mLINEITEM1_L4_1_E1_1_entry, long, COUNT_mLINEITEM1_L4_1_E1_1_mapkey0_idxfn, true>
  > COUNT_mLINEITEM1_L4_1_E1_1_map;
  typedef HashIndex<COUNT_mLINEITEM1_L4_1_E1_1_entry, long, COUNT_mLINEITEM1_L4_1_E1_1_mapkey0_idxfn, true> HashIndex_COUNT_mLINEITEM1_L4_1_E1_1_map_0;
  
  
  struct COUNT_mLINEITEM1_L4_1_L2_1_entry {
    long R2_SUPPKEY; DOUBLE_TYPE __av; COUNT_mLINEITEM1_L4_1_L2_1_entry* nxt; COUNT_mLINEITEM1_L4_1_L2_1_entry* prv;
  
    explicit COUNT_mLINEITEM1_L4_1_L2_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_mLINEITEM1_L4_1_L2_1_entry(const long c0, const DOUBLE_TYPE c1) { R2_SUPPKEY = c0; __av = c1;  }
    COUNT_mLINEITEM1_L4_1_L2_1_entry(const COUNT_mLINEITEM1_L4_1_L2_1_entry& other) : R2_SUPPKEY(other.R2_SUPPKEY), __av(other.__av), nxt(nullptr), prv(nullptr) { }
    
    
    FORCE_INLINE COUNT_mLINEITEM1_L4_1_L2_1_entry& modify(const long c0) { R2_SUPPKEY = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, R2_SUPPKEY);
      ar << ELEM_SEPARATOR;
      DBT_SERIALIZATION_NVP(ar, __av);
    }
  };
  
  struct COUNT_mLINEITEM1_L4_1_L2_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_mLINEITEM1_L4_1_L2_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.R2_SUPPKEY);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_mLINEITEM1_L4_1_L2_1_entry& x, const COUNT_mLINEITEM1_L4_1_L2_1_entry& y) {
      return x.R2_SUPPKEY == y.R2_SUPPKEY;
    }
  };
  
  typedef MultiHashMap<COUNT_mLINEITEM1_L4_1_L2_1_entry, DOUBLE_TYPE,
    HashIndex<COUNT_mLINEITEM1_L4_1_L2_1_entry, DOUBLE_TYPE, COUNT_mLINEITEM1_L4_1_L2_1_mapkey0_idxfn, true>
  > COUNT_mLINEITEM1_L4_1_L2_1_map;
  typedef HashIndex<COUNT_mLINEITEM1_L4_1_L2_1_entry, DOUBLE_TYPE, COUNT_mLINEITEM1_L4_1_L2_1_mapkey0_idxfn, true> HashIndex_COUNT_mLINEITEM1_L4_1_L2_1_map_0;

  

  /* Defines top-level materialized views */
  struct tlq_t {
    struct timeval t0, t; long tT, tN, tS;
    tlq_t(): tN(0), tS(0)  { 
      gettimeofday(&t0, NULL); 
    }
  
    /* Serialization code */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) const {
      ar << "\n";
      const COUNT_map& _COUNT = get_COUNT();
      dbtoaster::serialize_nvp_tabbed(ar, STRING(COUNT), _COUNT, "\t");
    }
  
    /* Functions returning / computing the results of top level queries */
    const COUNT_map& get_COUNT() const {
      return COUNT;
    }
  
  protected:
    /* Data structures used for storing / computing top-level queries */
    COUNT_map COUNT;
    
  };

  /* Contains materialized views and processing (IVM) logic */
  struct data_t : tlq_t {
  
    data_t(): tlq_t() {
      c2 = Udate(STRING_TYPE("1996-4-1"));
      c1 = Udate(STRING_TYPE("1996-1-1"));
      
    }
  
    
  
    /* Registering relations and trigger functions */
    ProgramBase* program_base;
    void register_data(ProgramBase& pb) {
      program_base = &pb;
    
      // Register maps
      pb.add_map<COUNT_map>("COUNT", COUNT);
      pb.add_map<COUNT_mLINEITEM1_map>("COUNT_mLINEITEM1", COUNT_mLINEITEM1);
      pb.add_map<COUNT_mLINEITEM1_E2_1_map>("COUNT_mLINEITEM1_E2_1", COUNT_mLINEITEM1_E2_1);
      pb.add_map<COUNT_mLINEITEM1_L3_1_map>("COUNT_mLINEITEM1_L3_1", COUNT_mLINEITEM1_L3_1);
      pb.add_map<COUNT_mLINEITEM1_L4_1_E1_1_map>("COUNT_mLINEITEM1_L4_1_E1_1", COUNT_mLINEITEM1_L4_1_E1_1);
      pb.add_map<COUNT_mLINEITEM1_L4_1_L2_1_map>("COUNT_mLINEITEM1_L4_1_L2_1", COUNT_mLINEITEM1_L4_1_L2_1);
    
      // Register streams and tables
      pb.add_relation("LINEITEM", false);
      pb.add_relation("SUPPLIER", false);
    
      // Register stream triggers
      pb.add_trigger("LINEITEM", insert_tuple, std::bind(&data_t::unwrap_insert_LINEITEM, this, std::placeholders::_1));
      pb.add_trigger("LINEITEM", delete_tuple, std::bind(&data_t::unwrap_delete_LINEITEM, this, std::placeholders::_1));
      pb.add_trigger("SUPPLIER", insert_tuple, std::bind(&data_t::unwrap_insert_SUPPLIER, this, std::placeholders::_1));
      pb.add_trigger("SUPPLIER", delete_tuple, std::bind(&data_t::unwrap_delete_SUPPLIER, this, std::placeholders::_1));
    
      
    
    }
  
    /* Trigger functions for table relations */
    
    
    /* Trigger functions for stream relations */
    void on_insert_LINEITEM(const long lineitem_orderkey, const long lineitem_partkey, const long lineitem_suppkey, const long lineitem_linenumber, const DOUBLE_TYPE lineitem_quantity, const DOUBLE_TYPE lineitem_extendedprice, const DOUBLE_TYPE lineitem_discount, const DOUBLE_TYPE lineitem_tax, const STRING_TYPE& lineitem_returnflag, const STRING_TYPE& lineitem_linestatus, const date lineitem_shipdate, const date lineitem_commitdate, const date lineitem_receiptdate, const STRING_TYPE& lineitem_shipinstruct, const STRING_TYPE& lineitem_shipmode, const STRING_TYPE& lineitem_comment) {
      
      ++tN;
      if ((lineitem_shipdate >= c1) && c2 > lineitem_shipdate) {
        COUNT_mLINEITEM1_E2_1.addOrDelOnZero(se1.modify(lineitem_suppkey), 1);
      
      }
      
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_L3_1.addOrDelOnZero(se2.modify(lineitem_suppkey), (lineitem_extendedprice * (1 + (-1 * lineitem_discount))));
      
      }
      
      if ((lineitem_shipdate >= c1) && c2 > lineitem_shipdate) {
        COUNT_mLINEITEM1_L4_1_E1_1.addOrDelOnZero(se3.modify(lineitem_suppkey), 1);
      
      }
      
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_L4_1_L2_1.addOrDelOnZero(se4.modify(lineitem_suppkey), (lineitem_extendedprice * (1 + (-1 * lineitem_discount))));
      
      }
      
      COUNT.clear();
      { //foreach
        COUNT_mLINEITEM1_entry* e1 = COUNT_mLINEITEM1.head;
        while (e1) {
          long s_suppkey = e1->S_SUPPKEY;
          STRING_TYPE s_name = e1->S_NAME;
          STRING_TYPE s_address = e1->S_ADDRESS;
          STRING_TYPE s_phone = e1->S_PHONE;
          long v1 = e1->__av;
          DOUBLE_TYPE l1 = COUNT_mLINEITEM1_L3_1.getValueOrDefault(se6.modify(s_suppkey));
          if ((COUNT_mLINEITEM1_E2_1.getValueOrDefault(se7.modify(s_suppkey)) != 0)) {
            int agg1 = 0;
            int agg2 = 0;
            { //foreach
              COUNT_mLINEITEM1_L4_1_E1_1_entry* e2 = COUNT_mLINEITEM1_L4_1_E1_1.head;
              while (e2) {
                long r2_suppkey = e2->R2_SUPPKEY;
                long v2 = e2->__av;
                if ((v2 != 0)) {
                  DOUBLE_TYPE l3 = COUNT_mLINEITEM1_L4_1_L2_1.getValueOrDefault(se8.modify(r2_suppkey));
                  (/*if */(l3 > l1) ? agg2 += 1 : 0);
                }
                e2 = e2->nxt;
              }
            }
            int l2 = agg2;
            (/*if */(l2 == 0) ? agg1 += 1 : 0);
            COUNT.addOrDelOnZero(se5.modify(s_suppkey, s_name, s_address, s_phone, l1), (v1 * agg1));
          }
          e1 = e1->nxt;
        }
      }
    }
    
    void unwrap_insert_LINEITEM(const event_args_t& ea) {
      on_insert_LINEITEM(*(reinterpret_cast<long*>(ea[0].get())), *(reinterpret_cast<long*>(ea[1].get())), *(reinterpret_cast<long*>(ea[2].get())), *(reinterpret_cast<long*>(ea[3].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[4].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[5].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[6].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[7].get())), *(reinterpret_cast<STRING_TYPE*>(ea[8].get())), *(reinterpret_cast<STRING_TYPE*>(ea[9].get())), *(reinterpret_cast<date*>(ea[10].get())), *(reinterpret_cast<date*>(ea[11].get())), *(reinterpret_cast<date*>(ea[12].get())), *(reinterpret_cast<STRING_TYPE*>(ea[13].get())), *(reinterpret_cast<STRING_TYPE*>(ea[14].get())), *(reinterpret_cast<STRING_TYPE*>(ea[15].get())));
    }
    
    void on_delete_LINEITEM(const long lineitem_orderkey, const long lineitem_partkey, const long lineitem_suppkey, const long lineitem_linenumber, const DOUBLE_TYPE lineitem_quantity, const DOUBLE_TYPE lineitem_extendedprice, const DOUBLE_TYPE lineitem_discount, const DOUBLE_TYPE lineitem_tax, const STRING_TYPE& lineitem_returnflag, const STRING_TYPE& lineitem_linestatus, const date lineitem_shipdate, const date lineitem_commitdate, const date lineitem_receiptdate, const STRING_TYPE& lineitem_shipinstruct, const STRING_TYPE& lineitem_shipmode, const STRING_TYPE& lineitem_comment) {
      
      ++tN;
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_E2_1.addOrDelOnZero(se9.modify(lineitem_suppkey), -1);
      
      }
      
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_L3_1.addOrDelOnZero(se10.modify(lineitem_suppkey), (-1 * (lineitem_extendedprice * (1 + (-1 * lineitem_discount)))));
      
      }
      
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_L4_1_E1_1.addOrDelOnZero(se11.modify(lineitem_suppkey), -1);
      
      }
      
      if ((lineitem_shipdate >= c1) && (c2 > lineitem_shipdate)) {
        COUNT_mLINEITEM1_L4_1_L2_1.addOrDelOnZero(se12.modify(lineitem_suppkey), (-1 * (lineitem_extendedprice * (1 + (-1 * lineitem_discount)))));
      
      }
      
      COUNT.clear();
      { //foreach
        COUNT_mLINEITEM1_entry* e3 = COUNT_mLINEITEM1.head;
        while (e3) {
          long s_suppkey = e3->S_SUPPKEY;
          STRING_TYPE s_name = e3->S_NAME;
          STRING_TYPE s_address = e3->S_ADDRESS;
          STRING_TYPE s_phone = e3->S_PHONE;
          long v3 = e3->__av;
          DOUBLE_TYPE l4 = COUNT_mLINEITEM1_L3_1.getValueOrDefault(se14.modify(s_suppkey));
          if ((COUNT_mLINEITEM1_E2_1.getValueOrDefault(se15.modify(s_suppkey)) != 0)) {
            int agg3 = 0;
            int agg4 = 0;
            { //foreach
              COUNT_mLINEITEM1_L4_1_E1_1_entry* e4 = COUNT_mLINEITEM1_L4_1_E1_1.head;
              while (e4) {
                long r2_suppkey = e4->R2_SUPPKEY;
                long v4 = e4->__av;
                if ((v4 != 0)) {
                  DOUBLE_TYPE l6 = COUNT_mLINEITEM1_L4_1_L2_1.getValueOrDefault(se16.modify(r2_suppkey));
                  (/*if */(l6 > l4) ? agg4 += 1 : 0);
                }
                e4 = e4->nxt;
              }
            }
            int l5 = agg4;
            (/*if */(l5 == 0) ? agg3 += 1 : 0);
            COUNT.addOrDelOnZero(se13.modify(s_suppkey, s_name, s_address, s_phone, l4), (v3 * agg3));
          }
          e3 = e3->nxt;
        }
      }
    }
    
    void unwrap_delete_LINEITEM(const event_args_t& ea) {
      on_delete_LINEITEM(*(reinterpret_cast<long*>(ea[0].get())), *(reinterpret_cast<long*>(ea[1].get())), *(reinterpret_cast<long*>(ea[2].get())), *(reinterpret_cast<long*>(ea[3].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[4].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[5].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[6].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[7].get())), *(reinterpret_cast<STRING_TYPE*>(ea[8].get())), *(reinterpret_cast<STRING_TYPE*>(ea[9].get())), *(reinterpret_cast<date*>(ea[10].get())), *(reinterpret_cast<date*>(ea[11].get())), *(reinterpret_cast<date*>(ea[12].get())), *(reinterpret_cast<STRING_TYPE*>(ea[13].get())), *(reinterpret_cast<STRING_TYPE*>(ea[14].get())), *(reinterpret_cast<STRING_TYPE*>(ea[15].get())));
    }
    
    void on_insert_SUPPLIER(const long supplier_suppkey, const STRING_TYPE& supplier_name, const STRING_TYPE& supplier_address, const long supplier_nationkey, const STRING_TYPE& supplier_phone, const DOUBLE_TYPE supplier_acctbal, const STRING_TYPE& supplier_comment) {
      
      ++tN;
      DOUBLE_TYPE l7 = COUNT_mLINEITEM1_L3_1.getValueOrDefault(se18.modify(supplier_suppkey));
      if ((COUNT_mLINEITEM1_E2_1.getValueOrDefault(se19.modify(supplier_suppkey)) != 0)) {
        int agg5 = 0;
        int agg6 = 0;
        { //foreach
          COUNT_mLINEITEM1_L4_1_E1_1_entry* e5 = COUNT_mLINEITEM1_L4_1_E1_1.head;
          while (e5) {
            long r2_suppkey = e5->R2_SUPPKEY;
            long v5 = e5->__av;
            if ((v5 != 0)) {
              DOUBLE_TYPE l9 = COUNT_mLINEITEM1_L4_1_L2_1.getValueOrDefault(se20.modify(r2_suppkey));
              (/*if */(l9 > l7) ? agg6 += 1 : 0);
            }
            e5 = e5->nxt;
          }
        }
        int l8 = agg6;
        (/*if */(l8 == 0) ? agg5 += 1 : 0);
        COUNT.addOrDelOnZero(se17.modify(supplier_suppkey, supplier_name, supplier_address, supplier_phone, l7), agg5);
      }
      
      COUNT_mLINEITEM1.addOrDelOnZero(se21.modify(supplier_suppkey, supplier_name, supplier_address, supplier_phone), 1);
    }
    
    void unwrap_insert_SUPPLIER(const event_args_t& ea) {
      on_insert_SUPPLIER(*(reinterpret_cast<long*>(ea[0].get())), *(reinterpret_cast<STRING_TYPE*>(ea[1].get())), *(reinterpret_cast<STRING_TYPE*>(ea[2].get())), *(reinterpret_cast<long*>(ea[3].get())), *(reinterpret_cast<STRING_TYPE*>(ea[4].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[5].get())), *(reinterpret_cast<STRING_TYPE*>(ea[6].get())));
    }
    
    void on_delete_SUPPLIER(const long supplier_suppkey, const STRING_TYPE& supplier_name, const STRING_TYPE& supplier_address, const long supplier_nationkey, const STRING_TYPE& supplier_phone, const DOUBLE_TYPE supplier_acctbal, const STRING_TYPE& supplier_comment) {
      
      ++tN;
      DOUBLE_TYPE l10 = COUNT_mLINEITEM1_L3_1.getValueOrDefault(se23.modify(supplier_suppkey));
      if ((COUNT_mLINEITEM1_E2_1.getValueOrDefault(se24.modify(supplier_suppkey)) != 0)) {
        int agg7 = 0;
        int agg8 = 0;
        { //foreach
          COUNT_mLINEITEM1_L4_1_E1_1_entry* e6 = COUNT_mLINEITEM1_L4_1_E1_1.head;
          while (e6) {
            long r2_suppkey = e6->R2_SUPPKEY;
            long v6 = e6->__av;
            if ((v6 != 0)) {
              DOUBLE_TYPE l12 = COUNT_mLINEITEM1_L4_1_L2_1.getValueOrDefault(se25.modify(r2_suppkey));
              (/*if */(l12 > l10) ? agg8 += 1 : 0);
            }
            e6 = e6->nxt;
          }
        }
        int l11 = agg8;
        (/*if */(l11 == 0) ? agg7 += 1 : 0);
        COUNT.addOrDelOnZero(se22.modify(supplier_suppkey, supplier_name, supplier_address, supplier_phone, l10), (agg7 * -1));
      }
      
      COUNT_mLINEITEM1.addOrDelOnZero(se26.modify(supplier_suppkey, supplier_name, supplier_address, supplier_phone), -1);
    }
    
    void unwrap_delete_SUPPLIER(const event_args_t& ea) {
      on_delete_SUPPLIER(*(reinterpret_cast<long*>(ea[0].get())), *(reinterpret_cast<STRING_TYPE*>(ea[1].get())), *(reinterpret_cast<STRING_TYPE*>(ea[2].get())), *(reinterpret_cast<long*>(ea[3].get())), *(reinterpret_cast<STRING_TYPE*>(ea[4].get())), *(reinterpret_cast<DOUBLE_TYPE*>(ea[5].get())), *(reinterpret_cast<STRING_TYPE*>(ea[6].get())));
    }
    
    void on_system_ready_event() {
      
    }
  
  private:
    
      /* Preallocated map entries (to avoid recreation of temporary objects) */
      COUNT_mLINEITEM1_E2_1_entry se1;
      COUNT_mLINEITEM1_L3_1_entry se2;
      COUNT_mLINEITEM1_L4_1_E1_1_entry se3;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se4;
      COUNT_entry se5;
      COUNT_mLINEITEM1_L3_1_entry se6;
      COUNT_mLINEITEM1_E2_1_entry se7;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se8;
      COUNT_mLINEITEM1_E2_1_entry se9;
      COUNT_mLINEITEM1_L3_1_entry se10;
      COUNT_mLINEITEM1_L4_1_E1_1_entry se11;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se12;
      COUNT_entry se13;
      COUNT_mLINEITEM1_L3_1_entry se14;
      COUNT_mLINEITEM1_E2_1_entry se15;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se16;
      COUNT_entry se17;
      COUNT_mLINEITEM1_L3_1_entry se18;
      COUNT_mLINEITEM1_E2_1_entry se19;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se20;
      COUNT_mLINEITEM1_entry se21;
      COUNT_entry se22;
      COUNT_mLINEITEM1_L3_1_entry se23;
      COUNT_mLINEITEM1_E2_1_entry se24;
      COUNT_mLINEITEM1_L4_1_L2_1_entry se25;
      COUNT_mLINEITEM1_entry se26;
    
      
    
      /* Data structures used for storing materialized views */
      COUNT_mLINEITEM1_map COUNT_mLINEITEM1;
      COUNT_mLINEITEM1_E2_1_map COUNT_mLINEITEM1_E2_1;
      COUNT_mLINEITEM1_L3_1_map COUNT_mLINEITEM1_L3_1;
      COUNT_mLINEITEM1_L4_1_E1_1_map COUNT_mLINEITEM1_L4_1_E1_1;
      COUNT_mLINEITEM1_L4_1_L2_1_map COUNT_mLINEITEM1_L4_1_L2_1;
    
      
    
      /* Constant definitions */
      /* const static */ date c2;
      /* const static */ date c1;
  };

  /* Type definition providing a way to execute the sql program */
  class Program : public ProgramBase {
    public:
      Program(int argc = 0, char* argv[] = 0) : ProgramBase(argc,argv) {
        data.register_data(*this);
  
        /* Specifying data sources */
        
        pair<string,string> source1_adaptor_params[] = { make_pair("delimiter", "|"), make_pair("schema", "long,long,long,long,double,double,double,double,string,string,date,date,date,string,string,string"), make_pair("deletions", "false") };
        std::shared_ptr<csv_adaptor> source1_adaptor(new csv_adaptor(get_relation_id("LINEITEM"), 3, source1_adaptor_params));
        frame_descriptor source1_fd("\n");
        std::shared_ptr<dbt_file_source> source1_file(new dbt_file_source("/home/qiuyuan/project/dbtoaster/tpch-data01/lineitem.tbl",source1_fd,source1_adaptor));
        add_source(source1_file, false);
        
        pair<string,string> source2_adaptor_params[] = { make_pair("delimiter", "|"), make_pair("schema", "long,string,string,long,string,double,string"), make_pair("deletions", "false") };
        std::shared_ptr<csv_adaptor> source2_adaptor(new csv_adaptor(get_relation_id("SUPPLIER"), 3, source2_adaptor_params));
        frame_descriptor source2_fd("\n");
        std::shared_ptr<dbt_file_source> source2_file(new dbt_file_source("/home/qiuyuan/project/dbtoaster/tpch-data01/supplier.tbl",source2_fd,source2_adaptor));
        add_source(source2_file, false);
      }
  
      /* Imports data for static tables and performs view initialization based on it. */
      void init() {
          table_multiplexer.init_source(run_opts->batch_size, run_opts->parallel, true);
          stream_multiplexer.init_source(run_opts->batch_size, run_opts->parallel, false);
  
          // struct timeval ts0, ts1, ts2;
          // gettimeofday(&ts0, NULL);
          process_tables();
          // gettimeofday(&ts1, NULL);
          // long int et1 = (ts1.tv_sec - ts0.tv_sec) * 1000L + (ts1.tv_usec - ts0.tv_usec) / 1000;
          // std::cout << "Populating static tables time: " << et1 << " (ms)" << std::endl;
  
          data.on_system_ready_event();
          // gettimeofday(&ts2, NULL);
          // long int et2 = (ts2.tv_sec - ts1.tv_sec) * 1000L + (ts2.tv_usec - ts1.tv_usec) / 1000;
          // std::cout << "OnSystemReady time: " << et2 << " (ms)" << std::endl;
  
          gettimeofday(&data.t0, NULL);
      }
  
      /* Saves a snapshot of the data required to obtain the results of top level queries. */
      snapshot_t take_snapshot() {
          // gettimeofday(&data.t, NULL);
          // long int t = (data.t.tv_sec - data.t0.tv_sec) * 1000L + (data.t.tv_usec - data.t0.tv_usec) / 1000;
          // std::cout << "Trigger running time: " << t << " (ms)" << std::endl;
          
          tlq_t* d = new tlq_t((tlq_t&) data);
          // gettimeofday(&(d->t), NULL);
          // d->tT = ((d->t).tv_sec - (d->t0).tv_sec) * 1000000L + ((d->t).tv_usec - (d->t0).tv_usec);
          // printf("SAMPLE = standard, %ld, %ld, %ld\n", d->tT, d->tN, d->tS);
          return snapshot_t( d );
      }
  
    protected:
      data_t data;
  };
  
  class Q15 : public Program {
    public:
      Q15(int argc = 0, char* argv[] = 0) : Program(argc, argv) { }
  };

}