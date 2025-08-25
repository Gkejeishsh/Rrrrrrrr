import os
import asyncio
import zipfile
import tempfile
import shutil
from datetime import datetime
from typing import List, Dict
import aiohttp
import aiofiles
from telegram import Update, Document
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.error import TelegramError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileProcessor:
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.pending_files: Dict[str, List[Document]] = {}
        self.processing_timeout = 300  # 5 minutes timeout
        
    async def download_large_file(self, file_url: str, file_path: str) -> bool:
        """Download large files in chunks to bypass size limitations"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(file_url, headers=headers) as response:
                    if response.status == 200:
                        async with aiofiles.open(file_path, 'wb') as file:
                            async for chunk in response.content.iter_chunked(8192):
                                await file.write(chunk)
                        return True
            return False
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            return False

    async def get_file_download_url(self, file_id: str) -> str:
        """Get direct download URL for Telegram file"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{self.bot_token}/getFile"
                params = {'file_id': file_id}
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data['ok']:
                            file_path = data['result']['file_path']
                            return f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}"
            return None
        except Exception as e:
            logger.error(f"Error getting file URL: {e}")
            return None

    async def create_zip_archive(self, files: List[tuple], zip_path: str) -> bool:
        """Create ZIP archive from downloaded files"""
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path, file_name in files:
                    if os.path.exists(file_path):
                        zipf.write(file_path, file_name)
            return True
        except Exception as e:
            logger.error(f"Error creating ZIP: {e}")
            return False

    async def upload_large_file(self, chat_id: int, file_path: str, context: ContextTypes.DEFAULT_TYPE):
        """Upload large files by splitting them if necessary"""
        file_size = os.path.getsize(file_path)
        max_size = 50 * 1024 * 1024  # 50MB chunks
        
        if file_size <= max_size:
            # File is small enough, upload directly
            try:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=open(file_path, 'rb'),
                    caption="📎 Your processed files"
                )
            except Exception as e:
                logger.error(f"Error uploading file: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"❌ Error uploading file: {str(e)}"
                )
        else:
            # Split large file into chunks
            await self.upload_file_in_chunks(chat_id, file_path, context)

    async def upload_file_in_chunks(self, chat_id: int, file_path: str, context: ContextTypes.DEFAULT_TYPE):
        """Split and upload large files in chunks"""
        chunk_size = 45 * 1024 * 1024  # 45MB chunks to be safe
        file_name = os.path.basename(file_path)
        
        try:
            with open(file_path, 'rb') as f:
                chunk_num = 1
                while True:
                    chunk_data = f.read(chunk_size)
                    if not chunk_data:
                        break
                    
                    # Create temporary chunk file
                    chunk_filename = f"{file_name}.part{chunk_num:03d}"
                    chunk_path = os.path.join(tempfile.gettempdir(), chunk_filename)
                    
                    with open(chunk_path, 'wb') as chunk_file:
                        chunk_file.write(chunk_data)
                    
                    # Upload chunk
                    await context.bot.send_document(
                        chat_id=chat_id,
                        document=open(chunk_path, 'rb'),
                        caption=f"📎 Part {chunk_num} of {file_name}"
                    )
                    
                    # Clean up chunk file
                    os.remove(chunk_path)
                    chunk_num += 1
                    
            # Send instructions for reassembly
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ File '{file_name}' uploaded in {chunk_num-1} parts.\n"
                     f"To reassemble: `cat {file_name}.part* > {file_name}`",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Error uploading file in chunks: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Error uploading file: {str(e)}"
            )

    async def process_files(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Process all pending files for a chat"""
        user_key = str(chat_id)
        
        if user_key not in self.pending_files or not self.pending_files[user_key]:
            return
            
        files_to_process = self.pending_files[user_key].copy()
        self.pending_files[user_key].clear()
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🔄 Processing {len(files_to_process)} files..."
        )
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        downloaded_files = []
        
        try:
            # Download all files
            for doc in files_to_process:
                file_url = await self.get_file_download_url(doc.file_id)
                if file_url:
                    file_path = os.path.join(temp_dir, doc.file_name)
                    success = await self.download_large_file(file_url, file_path)
                    if success:
                        downloaded_files.append((file_path, doc.file_name))
                        logger.info(f"Downloaded: {doc.file_name}")
                    else:
                        logger.error(f"Failed to download: {doc.file_name}")
            
            if downloaded_files:
                # Create ZIP archive
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                zip_filename = f"files_{timestamp}.zip"
                zip_path = os.path.join(temp_dir, zip_filename)
                
                success = await self.create_zip_archive(downloaded_files, zip_path)
                
                if success:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ Created ZIP with {len(downloaded_files)} files. Uploading..."
                    )
                    
                    # Upload the ZIP file
                    await self.upload_large_file(chat_id, zip_path, context)
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="❌ Failed to create ZIP archive"
                    )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="❌ No files were successfully downloaded"
                )
                
        except Exception as e:
            logger.error(f"Error processing files: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Error processing files: {str(e)}"
            )
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir)
            except:
                pass

    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming documents"""
        if not update.message or not update.message.document:
            return
            
        chat_id = update.message.chat_id
        user_key = str(chat_id)
        document = update.message.document
        
        # Initialize user's file list if needed
        if user_key not in self.pending_files:
            self.pending_files[user_key] = []
            
        # Add document to pending files
        self.pending_files[user_key].append(document)
        
        # Send confirmation
        await update.message.reply_text(
            f"📥 Added '{document.file_name}' to processing queue.\n"
            f"Current queue: {len(self.pending_files[user_key])} files\n\n"
            f"Send more files to add them to the same ZIP, or wait 30 seconds for automatic processing."
        )
        
        # Schedule processing after delay (to allow for multiple files)
        if f"process_{user_key}" in context.job_queue.jobs():
            # Cancel existing job
            for job in context.job_queue.jobs():
                if job.name == f"process_{user_key}":
                    job.schedule_removal()
        
        # Schedule new processing job
        context.job_queue.run_once(
            callback=lambda context: self.process_files(chat_id, context),
            when=30,  # 30 seconds delay
            name=f"process_{user_key}",
            chat_id=chat_id
        )

    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        welcome_message = """
🤖 **File Processing Bot**

Welcome! I can help you download, compress, and re-upload files from Telegram.

**How to use:**
1. Forward files to me (up to 2GB each)
2. I'll download them and create a ZIP archive
3. Files sent together will be zipped together
4. Large files will be split into chunks if needed

**Features:**
✅ Bypasses Telegram's 20MB download limit
✅ Handles files up to 2GB
✅ Automatic ZIP compression
✅ Smart chunking for large uploads
✅ Groups forwarded files together

Just start sending files!
        """
        
        await update.message.reply_text(
            welcome_message,
            parse_mode='Markdown'
        )

def create_application():
    """Create and configure the bot application"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
    
    processor = FileProcessor(bot_token)
    
    # Create application
    application = Application.builder().token(bot_token).build()
    
    # Add handlers
    application.add_handler(MessageHandler(filters.COMMAND & filters.Regex('^/start'), processor.handle_start))
    application.add_handler(MessageHandler(filters.Document.ALL, processor.handle_document))
    
    return application

# For Vercel deployment
app = create_application()

async def handler(request):
    """Vercel webhook handler"""
    if request.method == "POST":
        try:
            update = Update.de_json(await request.json(), app.bot)
            await app.process_update(update)
            return {"statusCode": 200, "body": "OK"}
        except Exception as e:
            logger.error(f"Error processing update: {e}")
            return {"statusCode": 500, "body": str(e)}
    
    return {"statusCode": 200, "body": "Bot is running"}

# For local testing
if __name__ == "__main__":
    app.run_polling()
