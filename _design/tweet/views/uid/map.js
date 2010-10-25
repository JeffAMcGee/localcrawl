function(doc) {
  if(doc.doc_type=="Tweet")
    emit(doc.uid,null);
}
