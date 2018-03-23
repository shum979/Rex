package rex.recon.writer

object InvalidDataStoreHandlerFactory {
  /**
    * Builds and retuens FileHandler with location and file name.
    *
    * @param location
    * @param fileName
    * @return
    */
  def getFileHandler(location: String, fileName: String): FileHandler = {
    new FileHandlerBuilder().withLocation(location).withFileName(fileName).build()
  }
}
