function(doc) {
  if(doc.doc_type=="User")
    emit(doc.sn,null);
}
