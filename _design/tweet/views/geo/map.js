function(doc) {
  if(doc.doc_type=="Tweet" && 'geo' in doc)
    emit(doc.uid,null);
}
