function(doc) {
  if(doc.doc_type=="Tweet")
    emit(doc.ca.slice(0,3),null);
}
