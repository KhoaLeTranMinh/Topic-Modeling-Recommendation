#!/usr/bin/env python3
"""
GPU Setup Script for Topic Modeling Recommendation System

This script helps set up GPU acceleration for the topic modeling notebooks.
It checks for CUDA availability, installs appropriate PyTorch versions,
and provides diagnostics for GPU optimization.
"""

import os
import sys
import subprocess
import platform
import torch
import importlib.util


def check_cuda_availability():
    """Check if CUDA is available and display GPU information"""
    print("🖥️  CUDA & GPU Detection:")
    print(f"   PyTorch version: {torch.__version__}")
    print(f"   CUDA available: {torch.cuda.is_available()}")
    
    if torch.cuda.is_available():
        print(f"   CUDA version: {torch.version.cuda}")
        print(f"   GPU count: {torch.cuda.device_count()}")
        
        for i in range(torch.cuda.device_count()):
            props = torch.cuda.get_device_properties(i)
            print(f"   GPU {i}: {props.name}")
            print(f"     Memory: {props.total_memory / (1024**3):.1f} GB")
            print(f"     Compute capability: {props.major}.{props.minor}")
    else:
        print("   ⚠️  No CUDA-capable GPU detected")
    
    return torch.cuda.is_available()


def check_nvidia_driver():
    """Check NVIDIA driver version"""
    print("\n🔧 NVIDIA Driver Check:")
    try:
        result = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            for line in lines:
                if 'Driver Version' in line:
                    print(f"   {line.strip()}")
                    break
        else:
            print("   ❌ nvidia-smi not found - NVIDIA drivers may not be installed")
    except FileNotFoundError:
        print("   ❌ nvidia-smi not found - NVIDIA drivers may not be installed")


def install_pytorch_gpu():
    """Install PyTorch with CUDA support"""
    print("\n🚀 PyTorch GPU Installation:")
    
    # Detect CUDA version
    cuda_version = None
    try:
        result = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'CUDA Version' in line:
                    cuda_version = line.split('CUDA Version: ')[1].split()[0]
                    break
    except:
        pass
    
    if cuda_version:
        print(f"   Detected CUDA version: {cuda_version}")
        
        # Choose appropriate PyTorch installation
        if cuda_version.startswith('12.'):
            install_cmd = [
                sys.executable, '-m', 'pip', 'install', 
                'torch', 
                '--index-url', 'https://download.pytorch.org/whl/cu121'
            ]
            print("   Installing PyTorch for CUDA 12.1...")
        elif cuda_version.startswith('11.'):
            install_cmd = [
                sys.executable, '-m', 'pip', 'install', 
                'torch',
                '--index-url', 'https://download.pytorch.org/whl/cu118'
            ]
            print("   Installing PyTorch for CUDA 11.8...")
        else:
            print(f"   Unknown CUDA version: {cuda_version}")
            print("   Please install PyTorch manually from https://pytorch.org/get-started/locally/")
            return False
        
        try:
            subprocess.run(install_cmd, check=True)
            print("   ✅ PyTorch GPU installation completed")
            return True
        except subprocess.CalledProcessError as e:
            print(f"   ❌ PyTorch installation failed: {e}")
            return False
    else:
        print("   ⚠️  Could not detect CUDA version")
        print("   Please install PyTorch manually from https://pytorch.org/get-started/locally/")
        return False


def install_gpu_requirements():
    """Install GPU-specific requirements"""
    print("\n📦 Installing GPU Requirements:")
    
    try:
        subprocess.run([
            sys.executable, '-m', 'pip', 'install', 
            '-r', 'requirements_gpu.txt'
        ], check=True)
        print("   ✅ GPU requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed to install requirements: {e}")
        return False


def optimize_gpu_settings():
    """Provide GPU optimization recommendations"""
    print("\n⚡ GPU Optimization Recommendations:")
    
    if torch.cuda.is_available():
        gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024**3)
        print(f"   GPU Memory: {gpu_memory:.1f} GB")
        
        if gpu_memory < 4:
            print("   📝 Low GPU memory detected (<4GB):")
            print("     - Use smaller batch sizes (16-32)")
            print("     - Reduce vocabulary size (1000-2000)")
            print("     - Use fewer topics (5-8)")
            print("     - Enable gradient checkpointing")
            
        elif gpu_memory < 8:
            print("   📝 Medium GPU memory detected (4-8GB):")
            print("     - Use moderate batch sizes (32-64)")
            print("     - Standard vocabulary size (2000-3000)")
            print("     - Use moderate topics (8-12)")
            
        else:
            print("   📝 High GPU memory detected (8GB+):")
            print("     - Use large batch sizes (64-128)")
            print("     - Large vocabulary size (3000+)")
            print("     - Use more topics (12-20)")
    
    print("\n🔧 Environment Variables:")
    print("   Set these in your shell or notebook:")
    print("   export TOKENIZERS_PARALLELISM=false")
    print("   export PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128")


def run_gpu_test():
    """Run a simple GPU test"""
    print("\n🧪 GPU Test:")
    
    try:
        if torch.cuda.is_available():
            # Test GPU tensor operations
            device = torch.device('cuda')
            x = torch.randn(1000, 1000, device=device)
            y = torch.randn(1000, 1000, device=device)
            z = torch.mm(x, y)
            
            print("   ✅ GPU tensor operations working")
            print(f"   📊 Test tensor shape: {z.shape}")
            print(f"   📊 GPU memory allocated: {torch.cuda.memory_allocated() / (1024**2):.1f} MB")
            
            # Clear GPU memory
            del x, y, z
            torch.cuda.empty_cache()
            
        else:
            print("   ⚠️  No GPU available for testing")
            
    except Exception as e:
        print(f"   ❌ GPU test failed: {e}")


def main():
    """Main setup function"""
    print("🚀 GPU Setup for Topic Modeling Recommendation System")
    print("=" * 60)
    
    # Check current environment
    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.system()} {platform.release()}")
    
    # Check NVIDIA driver
    check_nvidia_driver()
    
    # Check current PyTorch
    cuda_available = check_cuda_availability()
    
    if not cuda_available:
        print("\n❓ Would you like to install PyTorch with GPU support? (y/n): ", end="")
        choice = input().lower().strip()
        
        if choice in ['y', 'yes']:
            success = install_pytorch_gpu()
            if success:
                print("\n🔄 Please restart your Python environment to use the new PyTorch installation")
                print("   In Jupyter: Kernel -> Restart Kernel")
                print("   In terminal: Restart your Python session")
    
    # Install other requirements
    if os.path.exists('requirements_gpu.txt'):
        print("\n❓ Would you like to install GPU requirements? (y/n): ", end="")
        choice = input().lower().strip()
        
        if choice in ['y', 'yes']:
            install_gpu_requirements()
    
    # Provide optimization tips
    optimize_gpu_settings()
    
    # Run GPU test
    if torch.cuda.is_available():
        run_gpu_test()
    
    print("\n✅ GPU setup complete!")
    print("\n🎯 Next steps:")
    print("   1. Restart your Python environment if you installed new packages")
    print("   2. Open the ctm_song_recommendation_gpu.ipynb notebook")
    print("   3. Run the GPU detection cell to verify everything works")


if __name__ == "__main__":
    main()
